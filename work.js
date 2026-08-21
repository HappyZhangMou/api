export default {
  async fetch(request, env, ctx) {
    // 全局兜底：任何异常都返回可读信息，避免 1101
    try {
      return await handleRequest(request, env, ctx);
    } catch (e) {
      return new Response(
        `Worker Error: ${e.message}\n\nStack:\n${e.stack || 'none'}`, 
        { status: 500, headers: { 'Content-Type': 'text/plain; charset=utf-8' } }
      );
    }
  },

  // 整点半自动触发聚合（需在 wrangler.toml 配置 crons = ["0 * * * *"]）
  async scheduled(event, env, ctx) {
    try {
      console.log('[Scheduled] Aggregation started at', new Date().toISOString());
      const output = await aggregateLinks(env, ctx);
      await writeCachedAggregate(env, output);
      console.log('[Scheduled] Aggregation completed, cached successfully.');
    } catch (e) {
      console.error('[Scheduled] Aggregation failed:', e.message);
    }
  }
};

async function handleRequest(request, env, ctx) {
  // 检查 KV 绑定
  if (!env || !env.LINKS_KV) {
    return new Response(
      'Error: KV binding "LINKS_KV" not found.\n\n请在 Cloudflare Dashboard > Worker Settings > KV Namespace Bindings 中绑定，或检查 wrangler.toml 配置。', 
      { status: 500, headers: { 'Content-Type': 'text/plain; charset=utf-8' } }
    );
  }

  const url = new URL(request.url);
  const corsHeaders = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type',
  };

  if (request.method === 'OPTIONS') {
    return new Response(null, { headers: corsHeaders });
  }

  if (url.pathname === '/save' && request.method === 'POST') {
    return handleSave(request, env, ctx, corsHeaders);
  }

  if (url.pathname === '/fetch') {
    return handleFetch(env, corsHeaders, ctx);
  }

  if (url.pathname === '/retry-failed' && request.method === 'POST') {
    return handleRetryFailed(env, ctx, corsHeaders);
  }

  if (url.pathname === '/' || url.pathname === '') {
    const accept = request.headers.get('Accept') || '';
    const userAgent = request.headers.get('User-Agent') || '';
    const isBrowser = accept.includes('text/html') || 
      /Mozilla|Chrome|Safari|Firefox|Edge/.test(userAgent);
    
    if (isBrowser) {
      return handleAdmin(env);
    } else {
      return handleFetch(env, corsHeaders, ctx);
    }
  }

  return handleAdmin(env);
}

async function handleSave(request, env, ctx, corsHeaders) {
  try {
    let links;
    const contentType = request.headers.get('content-type') || '';
    
    if (contentType.includes('application/x-www-form-urlencoded') || 
        contentType.includes('multipart/form-data')) {
      const formData = await request.formData();
      links = formData.get('links') || '';
    } else {
      links = await request.text();
    }
    
    await env.LINKS_KV.put('links', links);
    
    // 保存后自动刷新缓存（后台处理 IP）
    try {
      const output = await aggregateLinks(env, ctx);
      await writeCachedAggregate(env, output);
    } catch (e) {
      console.error('Auto refresh after save failed:', e.message);
    }
    
    return new Response('OK', { 
      status: 200,
      headers: { ...corsHeaders, 'Content-Type': 'text/plain' }
    });
  } catch (e) {
    return new Response('Save Error: ' + e.message, { 
      status: 500,
      headers: { ...corsHeaders, 'Content-Type': 'text/plain; charset=utf-8' }
    });
  }
}

// ==================== 核心聚合逻辑（两阶段：先快速缓存，后台处理 IP） ====================

async function aggregateLinks(env, ctx) {
  const links = await env.LINKS_KV.get('links');
  if (!links || !links.trim()) {
    throw new Error('No links configured. Please open this page in browser and add links first.');
  }
  
  const urls = links.split('\n')
    .map(l => l.trim())
    .filter(l => l && !l.startsWith('#'));
  
  if (urls.length === 0) {
    throw new Error('No valid links found (all lines are empty or comments).');
  }

  const seen = new Set();
  const allLines = [];
  let successCount = 0;
  const errors = [];

  // 1. 拉取所有上游链接（带超时）
  for (const url of urls) {
    try {
      const response = await fetchWithTimeout(url, {
        headers: { 
          'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        },
        cf: { cacheTtl: 60 }
      }, 8000);
      
      if (!response.ok) {
        errors.push(`# Error: HTTP ${response.status} for ${url}`);
        continue;
      }
      
      const text = await response.text();
      successCount++;
      
      const lines = text.split('\n');
      for (const line of lines) {
        const idx = line.indexOf('#');
        const processed = idx >= 0 ? line.substring(0, idx).trim() : line.trim();
        
        if (processed && !seen.has(processed)) {
          seen.add(processed);
          allLines.push(processed);
        }
      }
    } catch (e) {
      errors.push(`# Error fetching ${url}: ${e.message}`);
    }
  }

  if (successCount === 0) {
    const errorOutput = errors.join('\n') || 'All links failed';
    throw new Error(errorOutput);
  }

  // 2. 给所有包含 IP 的行添加 #未处理 标记
  const markedLines = allLines.map(line => {
    const ip = extractIp(line);
    if (ip) {
      return `${line}#未处理`;
    }
    return line;
  });

  // 3. 组装初始输出并立即写入缓存
  let output = markedLines.join('\n');
  if (errors.length > 0) {
    output += '\n\n' + errors.join('\n');
  }

  // 立即写入缓存，让外部请求可以获取到（带 #未处理 标记）
  await writeCachedAggregate(env, output);

  // 4. 启动后台任务：按队列处理 IP 归属地
  if (ctx) {
    ctx.waitUntil(processIpQueue(env));
  }

  return output;
}

// 辅助：统一写入 cached_aggregate 和 metadata，TTL 设置为 3 小时，返回写入时间戳
async function writeCachedAggregate(env, output, ttl = 60 * 60 * 3) {
  try {
    const now = Date.now();
    await env.LINKS_KV.put('cached_aggregate', output, { expirationTtl: ttl });
    await env.LINKS_KV.put('cached_aggregate_meta', String(now), { expirationTtl: ttl });
    return now;
  } catch (e) {
    console.error('[Cache] writeCachedAggregate failed:', e.message);
    throw e;
  }
}

// 条件写入：仅当当前缓存元信息时间不晚于 providedMinMeta 时才写入
async function conditionalWriteCachedAggregate(env, output, providedMinMeta, ttl = 60 * 60 * 3) {
  try {
    const currentMetaRaw = await env.LINKS_KV.get('cached_aggregate_meta');
    const currentMeta = currentMetaRaw ? parseInt(currentMetaRaw, 10) : 0;
    const minMeta = providedMinMeta || 0;
    if (currentMeta > minMeta) {
      console.log('[Cache] conditionalWrite skipped: newer cache exists');
      return false;
    }
    await writeCachedAggregate(env, output, ttl);
    return true;
  } catch (e) {
    console.error('[Cache] conditionalWriteCachedAggregate failed:', e.message);
    throw e;
  }
}

// ==================== 后台 IP 处理队列 ====================

async function processIpQueue(env) {
  try {
    let processedCount = 0;
    let failedCount = 0;
    let skippedCount = 0;
    let retryLaterCount = 0;
    let deletedCount = 0;
    let batchCount = 0;
    const MAX_BATCH = 100; // 每批处理 100 个 IP 再写回 KV，大幅减少 KV 操作次数

    while (true) {
      // 读取当前缓存
      const current = await env.LINKS_KV.get('cached_aggregate');
      if (!current) break;

      const lines = current.split('\n');
      const pendingItems = [];

      // 收集本批次待处理的项（#未处理 或旧版 #UN未知）
      for (let i = 0; i < lines.length && pendingItems.length < MAX_BATCH; i++) {
        const line = lines[i];
        let contentWithoutMark = null;
        let isUnknown = false;

        if (line.endsWith('#未处理')) {
          contentWithoutMark = line.slice(0, -4); // 去掉 #未处理（4个字符）
        } else if (line.endsWith('#UN未知')) {
          contentWithoutMark = line.slice(0, -5); // 去掉 #UN未知（5个字符）
          isUnknown = true;
        }

        if (contentWithoutMark !== null) {
          const ip = extractIp(contentWithoutMark);
          if (ip) {
            pendingItems.push({ index: i, line: contentWithoutMark, ip, isUnknown });
          } else {
            // 提取不到 IP，移除标记
            lines[i] = contentWithoutMark;
            skippedCount++;
          }
        }
      }

      if (pendingItems.length === 0) {
        // 没有待处理项，检查是否有因 skipped 修改的行需要写回
        if (skippedCount > 0) {
          const filteredLines = lines.filter(line => line !== null);
          await writeCachedAggregate(env, filteredLines.join('\n'));
        }
        break;
      }

      // 批量查询 IP 归属地（并发处理以加速完成）
      const CONCURRENCY = 3; // 并发数（Free 计划下保持较低并发）
      const processItem = async (item) => {
        let location;
        try {
          if (item.isUnknown) {
            const cleanIp = getCleanIp(item.ip);
            ipCache.delete(cleanIp);
          }
          location = await getIpLocation(item.ip, env);
        } catch (e) {
          console.error(`[Queue] Exception querying ${item.ip}:`, e.message);
          location = 'UN未知';
        }

        if (location === 'UN未知') {
          lines[item.index] = `${item.line}#分析失败-待重试`;
          // 使用原子更新计数
          retryLaterCount++;
          console.log(`[Queue] Failed for ${item.ip}: all APIs exhausted, will retry in 1 minute`);
        } else if (location.startsWith('CN')) {
          lines[item.index] = null;
          deletedCount++;
          console.log(`[Queue] Deleted ${item.ip}: China IP`);
        } else {
          lines[item.index] = `${item.line}#${location}`;
          processedCount++;
          console.log(`[Queue] Success for ${item.ip}: ${location}`);
        }
      };

      for (let i = 0; i < pendingItems.length; i += CONCURRENCY) {
        const batch = pendingItems.slice(i, i + CONCURRENCY);
        await Promise.all(batch.map(processItem));
      }

      // 批量写回缓存（过滤掉已删除的行）
      const filteredLines = lines.filter(line => line !== null);
      // 使用条件写入：传入当前轮次的 meta 时间戳，避免被更早的并发写覆盖
      const metaTs = Date.now();
      await conditionalWriteCachedAggregate(env, filteredLines.join('\n'), metaTs);
      batchCount++;
    }

    console.log(`[Queue] First pass completed. Batches: ${batchCount}, Processed: ${processedCount}, Failed: ${retryLaterCount}, Deleted: ${deletedCount}, Skipped: ${skippedCount}`);

    // 第二轮：重试 #分析失败-待重试 的项（等待 1 分钟后）
    let retryProcessed = 0;
    let retryFailed = 0;
    let retryDeleted = 0;
    let retryBatches = 0;
    
    if (retryLaterCount > 0) {
      console.log('[Queue] Waiting 1 minute before retrying failed items...');
      await sleep(60000);

      while (true) {
        const current = await env.LINKS_KV.get('cached_aggregate');
        if (!current) break;

        const lines = current.split('\n');
        const retryItems = [];
        let retrySkipped = 0;

        for (let i = 0; i < lines.length && retryItems.length < MAX_BATCH; i++) {
          const line = lines[i];
          if (line.endsWith('#分析失败-待重试')) {
            const contentWithoutMark = line.slice(0, -9); // 去掉 #分析失败-待重试（9个字符）
            const ip = extractIp(contentWithoutMark);
            if (ip) {
              retryItems.push({ index: i, line: contentWithoutMark, ip });
            } else {
              lines[i] = contentWithoutMark;
              retrySkipped++;
            }
          }
        }

        if (retryItems.length === 0) {
          if (retrySkipped > 0) {
            const filteredLines = lines.filter(line => line !== null);
            const metaTs2 = Date.now();
            await conditionalWriteCachedAggregate(env, filteredLines.join('\n'), metaTs2);
          }
          break;
        }

          // 并发重试处理（与首次处理相同的并发策略）
          const CONCURRENCY_RETRY = 3; // 重试并发也设为较低值以适应 Free 计划
          const processRetryItem = async (item) => {
            const cleanIp = getCleanIp(item.ip);
            ipCache.delete(cleanIp);
            let location;
            try {
              location = await getIpLocation(item.ip, env);
            } catch (e) {
              console.error(`[Queue Retry] Exception querying ${item.ip}:`, e.message);
              location = 'UN未知';
            }

            if (location === 'UN未知') {
              lines[item.index] = `${item.line}#分析失败`;
              retryFailed++;
              console.log(`[Queue Retry] Final fail for ${item.ip}: all APIs exhausted after retry`);
            } else if (location.startsWith('CN')) {
              lines[item.index] = null;
              retryDeleted++;
              console.log(`[Queue Retry] Deleted ${item.ip}: China IP`);
            } else {
              lines[item.index] = `${item.line}#${location}`;
              retryProcessed++;
              console.log(`[Queue Retry] Success for ${item.ip}: ${location}`);
            }
          };

          for (let i = 0; i < retryItems.length; i += CONCURRENCY_RETRY) {
            const batch = retryItems.slice(i, i + CONCURRENCY_RETRY);
            await Promise.all(batch.map(processRetryItem));
          }

        const filteredLines = lines.filter(line => line !== null);
        const metaTs3 = Date.now();
        await conditionalWriteCachedAggregate(env, filteredLines.join('\n'), metaTs3);
        retryBatches++;
      }

      console.log(`[Queue] Retry pass completed. Batches: ${retryBatches}, Processed: ${retryProcessed}, Failed: ${retryFailed}, Deleted: ${retryDeleted}`);
    }

    console.log(`[Queue] All done. Total processed: ${processedCount + retryProcessed}, Total failed: ${retryFailed}, Deleted: ${deletedCount + retryDeleted}, Skipped: ${skippedCount}`);
  } catch (e) {
    console.error('[Queue] Error in processIpQueue:', e.message);
  }
}

// 睡眠辅助函数
function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

async function handleRetryFailed(env, ctx, corsHeaders) {
  try {
    const cached = await env.LINKS_KV.get('cached_aggregate');
    if (!cached) {
      return new Response('No cache found', { 
        status: 404,
        headers: { ...corsHeaders, 'Content-Type': 'text/plain; charset=utf-8' }
      });
    }

    const lines = cached.split('\n');
    let retryCount = 0;

    const updatedLines = lines.map(line => {
      if (line.endsWith('#分析失败')) {
        retryCount++;
        return line.slice(0, -5) + '#未处理'; // 去掉 #分析失败（5个字符），加上 #未处理
      }
      return line;
    });

    if (retryCount === 0) {
      return new Response('没有分析失败的 IP 需要重试', {
        headers: { ...corsHeaders, 'Content-Type': 'text/plain; charset=utf-8' }
      });
    }

    const updated = updatedLines.join('\n');
    await writeCachedAggregate(env, updated);

    // 启动后台重新处理
    if (ctx) {
      ctx.waitUntil(processIpQueue(env));
    }

    return new Response(`已将 ${retryCount} 个分析失败的 IP 重新加入队列，后台开始处理`, {
      headers: { ...corsHeaders, 'Content-Type': 'text/plain; charset=utf-8' }
    });
  } catch (e) {
    return new Response('Retry Error: ' + e.message, { 
      status: 500,
      headers: { ...corsHeaders, 'Content-Type': 'text/plain; charset=utf-8' }
    });
  }
}

// ==================== /fetch 端点（直接返回当前缓存） ====================

async function handleFetch(env, corsHeaders, ctx) {
  // 始终直接返回当前缓存内容（可能包含 #未处理、已完成分析、#分析失败 的混合状态）
  try {
    const cached = await env.LINKS_KV.get('cached_aggregate');
    if (cached !== null && cached !== undefined) {
      return new Response(cached, {
        headers: { 
          ...corsHeaders, 
          'Content-Type': 'text/plain; charset=utf-8',
          'Cache-Control': 'no-cache',
          'X-Cache-Status': 'HIT'
        }
      });
    }
  } catch (e) {
    console.error('Cache read error:', e.message);
  }
  
  // 缓存完全不存在，实时聚合（同步模式，无后台处理）
  try {
    // 缓存不存在：同步聚合并在有 ctx 时启动后台 IP 处理
    const output = await aggregateLinks(env, ctx);
    // aggregateLinks 会写入缓存并在有 ctx 时启动后台处理
    return new Response(output, {
      headers: { 
        ...corsHeaders, 
        'Content-Type': 'text/plain; charset=utf-8',
        'Cache-Control': 'no-cache',
        'X-Cache-Status': 'MISS'
      }
    });
  } catch (e) {
    return new Response(e.message, { 
      status: 502,
      headers: { ...corsHeaders, 'Content-Type': 'text/plain; charset=utf-8' }
    });
  }
}

// ==================== IP 查询相关（保持原有逻辑） ====================

// IP 缓存，避免重复查询
const ipCache = new Map();

// API 退避（当某个 API 返回 429 时，记录退避到期时间）
const apiBackoff = new Map();
const API_BACKOFF_MS = 60 * 1000; // 1 分钟退避

// 主 API 速率限制（轻量、本实例内）：每分钟可保留调用数
const apiUsage = new Map();
const API_WINDOW_MS = 60 * 1000;
const API_PRIMARY_LIMIT = 30; // 主 API 每分钟上限（近似）

// 国家代码到中文的映射（常用）
const countryCodeMap = {
  'CN': '中国', 'US': '美国', 'JP': '日本', 'KR': '韩国', 'SG': '新加坡',
  'HK': '香港', 'TW': '台湾', 'GB': '英国', 'DE': '德国', 'FR': '法国',
  'RU': '俄罗斯', 'CA': '加拿大', 'AU': '澳大利亚', 'IN': '印度', 'BR': '巴西',
  'NL': '荷兰', 'SE': '瑞典', 'CH': '瑞士', 'IT': '意大利', 'ES': '西班牙',
  'PL': '波兰', 'TR': '土耳其', 'ID': '印尼', 'VN': '越南', 'TH': '泰国',
  'MY': '马来西亚', 'PH': '菲律宾', 'UA': '乌克兰', 'MX': '墨西哥', 'AE': '阿联酋',
  'ZA': '南非', 'SA': '沙特', 'AR': '阿根廷', 'BE': '比利时', 'AT': '奥地利',
  'IL': '以色列', 'IE': '爱尔兰', 'PT': '葡萄牙', 'FI': '芬兰', 'NO': '挪威',
  'DK': '丹麦', 'CZ': '捷克', 'HU': '匈牙利', 'RO': '罗马尼亚', 'BG': '保加利亚',
  'SK': '斯洛伐克', 'LT': '立陶宛', 'LV': '拉脱维亚', 'EE': '爱沙尼亚', 'HR': '克罗地亚',
  'SI': '斯洛文尼亚', 'GR': '希腊', 'IS': '冰岛', 'LU': '卢森堡', 'MT': '马耳他',
  'CY': '塞浦路斯', 'LI': '列支敦士登', 'MC': '摩纳哥', 'AD': '安道尔', 'SM': '圣马力诺',
  'VA': '梵蒂冈', 'MD': '摩尔多瓦', 'BY': '白俄罗斯', 'GE': '格鲁吉亚', 'AM': '亚美尼亚',
  'AZ': '阿塞拜疆', 'KZ': '哈萨克斯坦', 'UZ': '乌兹别克', 'KG': '吉尔吉斯', 'TJ': '塔吉克',
  'TM': '土库曼', 'MN': '蒙古', 'KP': '朝鲜', 'BD': '孟加拉', 'LK': '斯里兰卡',
  'NP': '尼泊尔', 'BT': '不丹', 'MV': '马尔代夫', 'PK': '巴基斯坦', 'AF': '阿富汗',
  'IR': '伊朗', 'IQ': '伊拉克', 'SY': '叙利亚', 'LB': '黎巴嫩', 'JO': '约旦',
  'PS': '巴勒斯坦', 'KW': '科威特', 'QA': '卡塔尔', 'BH': '巴林', 'OM': '阿曼',
  // 依次尝试多个 API；优先使用可用性更稳定的公开接口
  const apis = [
    {
      name: 'uapis.cn',
      handler: async () => {
        const response = await fetchWithTimeout(`https://uapis.cn/api/v1/network/ipinfo?ip=${cleanIp}`, { cf: { cacheTtl: 86400 } }, 3000);
        if (!response.ok) throw new Error(`HTTP ${response.status}`);
        return await response.json();
      },
      extract: (data) => {
        if (data && data.region) return data.region.trim();
        return null;
      },
    },
    {
      name: 'ipwho.is',
      handler: async () => {
        const response = await fetchWithTimeout(`https://ipwho.is/${cleanIp}?output=json`, { cf: { cacheTtl: 86400 } }, 3000);
        if (!response.ok) throw new Error(`HTTP ${response.status}`);
        return await response.json();
      },
      extract: (data) => {
        if (!data) return null;
        return data.country_code || data.country || data.country_code2 || null;
      }
    },
    {
      name: 'ifconfig.co',
      handler: async () => {
        const response = await fetchWithTimeout(`https://ifconfig.co/json?ip=${cleanIp}`, { cf: { cacheTtl: 86400 } }, 3000);
        if (!response.ok) throw new Error(`HTTP ${response.status}`);
        return await response.json();
      },
      extract: (data) => {
        if (!data) return null;
        return data.country_iso || data.country || null;
      }
    },
    {
      name: 'api.ip.sb',
      handler: async () => {
        const response = await fetchWithTimeout(`https://api.ip.sb/geoip/${cleanIp}`, { cf: { cacheTtl: 86400 } }, 3000);
        if (!response.ok) throw new Error(`HTTP ${response.status}`);
        return await response.json();
      },
      extract: (data) => {
        if (!data) return null;
        return data.country_code || data.country || null;
      }
    }
  ];

  for (const api of apis) {
    const apiName = api.name;
    // 如果该 API 在退避期，跳过
    const backoffUntil = apiBackoff.get(apiName) || 0;
    if (backoffUntil > Date.now()) {
      continue;
    }

    try {
      // 对主 API 做本地速率限制（近似），超出则直接切换到下一个备用 API
      if (apiName === 'uapis.cn') {
        const now = Date.now();
        const usage = apiUsage.get(apiName) || { windowStart: now, count: 0 };
        if (now - usage.windowStart > API_WINDOW_MS) {
          usage.windowStart = now;
          usage.count = 0;
        }
        if (usage.count >= API_PRIMARY_LIMIT) {
          // 标记短期退避并跳到下一个 API
          apiBackoff.set(apiName, Date.now() + API_BACKOFF_MS);
          console.log(`[IP API] ${apiName} local rate limit reached, backing off`);
          continue;
        }
        usage.count++;
        apiUsage.set(apiName, usage);
      }

      const data = await api.handler();
      const extracted = api.extract(data);
      if (extracted) {
        // 对 uapis.cn 做特殊匹配以返回代码+中文名
        if (apiName === 'uapis.cn') {
          let region = extracted;
          let code = 'UN';
          for (const [k, v] of Object.entries(countryCodeMap)) {
            if (region.includes(v)) { code = k; break; }
          }
          if (code === 'UN') {
            if (/^(美国|USA?|United States)/.test(region)) code = 'US';
            else if (/^(日本|Japan|JP)/.test(region)) code = 'JP';
            else if (/^(韩国|Korea|KR)/.test(region)) code = 'KR';
            else if (/^(新加坡|Singapore|SG)/.test(region)) code = 'SG';
            else if (/^(香港|Hong Kong|HK)/.test(region)) code = 'HK';
            else if (/^(台湾|Taiwan|TW)/.test(region)) code = 'TW';
            else if (/^(英国|United Kingdom|GB|UK)/.test(region)) code = 'GB';
            else if (/^(德国|Germany|DE)/.test(region)) code = 'DE';
            else if (/^(法国|France|FR)/.test(region)) code = 'FR';
            else if (/^(俄罗斯|Russia|RU)/.test(region)) code = 'RU';
            else if (/^(加拿大|Canada|CA)/.test(region)) code = 'CA';
            else if (/^(澳大利亚|Australia|AU)/.test(region)) code = 'AU';
            else if (/^(中国|China|CN)/.test(region)) code = 'CN';
          }
          const countryName = countryCodeMap[code] || region.split(/\s+/)[0];
          const result = `${code}${countryName}`;
          ipCache.set(cleanIp, result);
          return result;
        }

        // 其他 API 返回 country code 或 country
        const code = typeof extracted === 'string' ? extracted : '';
        const countryName = countryCodeMap[code] || (typeof extracted === 'string' ? extracted : '未知');
        const result = `${code}${countryName}`;
        ipCache.set(cleanIp, result);
        return result;
      }
      // 若未提取到数据，则当作失败继续尝试下个 API
    } catch (e) {
      const msg = (e && e.message) ? e.message : '';
      // 若为速率限制，设置退避并立即尝试下一个备用 API
      if (msg.includes('429') || /Too Many Requests/i.test(msg)) {
        apiBackoff.set(apiName, Date.now() + API_BACKOFF_MS);
        console.log(`[IP API] ${apiName} rate-limited, backing off for ${API_BACKOFF_MS}ms`);
        continue;
      }
      // 其他错误，记录并继续
      console.error(`[IP API] ${apiName} error:`, msg);
      continue;
    }
  }
        }, 3000);
        if (!response.ok) throw new Error(`HTTP ${response.status}`);
        const data = await response.json();
        if (data.country_code || data.country) {
          const code = data.country_code || data.country || '';
          const countryName = countryCodeMap[code] || data.country || '未知';
          // 统计功能已移除
          return `${code}${countryName}`;
        }
        throw new Error('No country data');
      } catch (e) {
        // 统计功能已移除
        throw e;
      }
    }
  ];
  
  for (const api of apis) {
    try {
      const result = await api();
      if (result === 'UN未知') {
        continue;
      }
      ipCache.set(cleanIp, result);
      return result;
    } catch (e) {
      continue;
    }
  }
  
  const result = 'UN未知';
  ipCache.set(cleanIp, result);
  return result;
}

// 清理 IP（去掉端口等），兼容 IPv4 和 IPv6
function getCleanIp(ip) {
  // IPv6 方括号格式: [2001:db8::1]:8080
  const ipv6BracketMatch = ip.match(/^\[([0-9a-fA-F:]+)\](?::\d+)?$/);
  if (ipv6BracketMatch) {
    return ipv6BracketMatch[1];
  }
  // IPv6 裸格式（理论上不应有端口，但做兼容）
  if (ip.includes(':') && !ip.includes('.')) {
    // 可能是 IPv6，直接返回（IPv6 本身含冒号，不应有端口后缀）
    return ip.split('%')[0].trim(); // 去掉 zone index
  }
  // IPv4 格式: 1.2.3.4:8080
  return ip.split(':')[0].trim();
}

// 从行中提取 IP 地址
function extractIp(line) {
  const ipv4Match = line.match(/\b(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})(:\d+)?\b/);
  if (ipv4Match && isValidIPv4(ipv4Match[1])) {
    return ipv4Match[1] + (ipv4Match[2] || '');
  }
  
  const ipv6BracketMatch = line.match(/\[([0-9a-fA-F:]+)\](?::\d+)?/);
  if (ipv6BracketMatch && isValidIPv6(ipv6BracketMatch[1])) {
    return ipv6BracketMatch[1];
  }
  
  const ipv6BareMatch = line.match(/\b([0-9a-fA-F]{1,4}(?::[0-9a-fA-F]{1,4}){2,7})\b/);
  if (ipv6BareMatch && isValidIPv6(ipv6BareMatch[1])) {
    return ipv6BareMatch[1];
  }
  
  return null;
}

// 验证 IPv4 地址是否合法
function isValidIPv4(ip) {
  const parts = ip.split('.');
  if (parts.length !== 4) return false;
  return parts.every(p => {
    const num = parseInt(p, 10);
    return p === String(num) && num >= 0 && num <= 255;
  });
}

// 验证 IPv6 地址是否合法（基本格式检查）
function isValidIPv6(ip) {
  if (!ip || ip.length < 2) return false;
  // 不能有两个以上的 ::
  const doubleColonCount = (ip.match(/::/g) || []).length;
  if (doubleColonCount > 1) return false;
  // 拆分各组
  const groups = ip.split(':');
  // 处理 :: 压缩的情况
  const validGroups = groups.filter(g => g !== '');
  if (doubleColonCount === 0 && validGroups.length !== 8) return false;
  if (doubleColonCount === 1 && validGroups.length >= 8) return false;
  return validGroups.every(g => /^[0-9a-fA-F]{1,4}$/.test(g));
}

// ==================== 管理后台 HTML ====================

async function handleAdmin(env) {
  const existing = await env.LINKS_KV.get('links') || '';
  const cached = await env.LINKS_KV.get('cached_aggregate');
  let cacheInfo = '暂无缓存';
  if (cached) {
    const allLines = cached.split('\n');
    let totalLines = 0;
    let unprocessed = 0;
    let failed = 0;
    let failedRetry = 0;
    
    for (const line of allLines) {
      const trimmed = line.trim();
      if (!trimmed || trimmed.startsWith('#')) continue;
      totalLines++;
      if (trimmed.endsWith('#未处理')) unprocessed++;
      else if (trimmed.endsWith('#分析失败')) failed++;
      else if (trimmed.endsWith('#分析失败-待重试')) failedRetry++;
    }
    
    const processed = totalLines - unprocessed - failed - failedRetry;
    cacheInfo = `已缓存（约 ${totalLines} 条记录，已处理 ${processed}，未处理 ${unprocessed}，待重试 ${failedRetry}，失败 ${failed}）`;
  }
  
  // API 成功率统计功能已移除
  const statsHtml = '';
  
  const html = `<!DOCTYPE html>
<html lang="zh-CN">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>链接聚合管理</title>
  <style>
    * { box-sizing: border-box; }
    body { 
      font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; 
        // 每3小时触发聚合（需在 wrangler.toml 配置 crons = ["0 */3 * * *"]）
      margin: 40px auto; 
      padding: 20px; 
      background: #f5f5f5;
      line-height: 1.6;
    }
    .container {
      background: white;
      padding: 30px;
      border-radius: 12px;
      box-shadow: 0 2px 10px rgba(0,0,0,0.1);
    }
    h1 { margin-top: 0; color: #333; }
    .info {
      background: #e3f2fd;
      padding: 15px;
      border-radius: 8px;
      margin-bottom: 20px;
      font-size: 14px;
      color: #1565c0;
    }
    .info code {
      background: #fff;
      padding: 2px 6px;
      border-radius: 4px;
      font-family: monospace;
      border: 1px solid #bbdefb;
    }
    /* API 查询成功率样式已移除 */
    .cache-status {
      background: #f3e5f5;
      padding: 12px 15px;
      border-radius: 8px;
      margin-bottom: 20px;
      font-size: 14px;
      color: #6a1b9a;
      display: flex;
      align-items: center;
      gap: 8px;
    }
    .cache-status::before {
      content: "⏱";
      font-size: 16px;
    }
    textarea { 
      width: 100%; 
      height: 350px; 
      font-family: monospace; 
      font-size: 14px;
      padding: 12px;
      border: 1px solid #ddd;
      border-radius: 8px;
      resize: vertical;
      line-height: 1.5;
    }
    textarea:focus {
      outline: none;
      border-color: #2196f3;
      box-shadow: 0 0 0 3px rgba(33,150,243,0.1);
    }
    .button-row {
      margin-top: 15px;
      display: flex;
      gap: 10px;
      align-items: center;
      flex-wrap: wrap;
    }
    button { 
      padding: 12px 28px; 
      cursor: pointer;
      background: #2196f3;
      color: white;
      border: none;
      border-radius: 8px;
      font-size: 16px;
      font-weight: 500;
      transition: all 0.2s;
    }
    button:hover { background: #1976d2; }
    button:disabled { background: #ccc; cursor: not-allowed; }
    .retry-btn {
      background: #ff9800;
    }
    .retry-btn:hover {
      background: #f57c00;
    }
    .refresh-btn {
      background: #4caf50;
    }
    .refresh-btn:hover {
      background: #388e3c;
    }
    .status { 
      padding: 12px 16px; 
      border-radius: 8px;
      font-size: 14px;
      display: none;
    }
    .success { background: #e8f5e9; color: #2e7d32; border: 1px solid #a5d6a7; }
    .error { background: #ffebee; color: #c62828; border: 1px solid #ef9a9a; }
    .tips {
      margin-top: 25px;
      padding: 20px;
      background: #fff8e1;
      border-radius: 8px;
      font-size: 14px;
      color: #5d4037;
    }
    .tips h3 { margin-top: 0; }
    .tips ul { margin: 10px 0; padding-left: 20px; }
    .tips li { margin: 6px 0; }
    .badge {
      display: inline-block;
      padding: 2px 8px;
      border-radius: 4px;
      font-size: 12px;
      font-weight: 600;
      margin-left: 6px;
    }
    .badge-new {
      background: #ff5722;
      color: white;
    }
  </style>
</head>
<body>
  <div class="container">
    <h1>🔗 链接聚合管理</h1>
    
    <div class="info">
      <strong>API 端点：</strong>程序请访问 <code>/fetch</code> 获取汇总后的内容<br>
      <strong>智能访问：</strong>直接 curl 根域名也会自动返回汇总内容<br>
      <strong>自动去重：</strong>多个链接返回的相同内容会自动去重，保留首次出现的顺序<br>
      <strong>IP 地区分析：</strong>自动识别每行中的 IP 地址并标注所属国家（如 <code>#CN中国</code>）
    </div>

    <!-- API 查询成功率板块已移除 -->

    <div class="cache-status">
      <strong>缓存状态：</strong>${cacheInfo} &nbsp;|&nbsp; 每整点自动更新
    </div>
    
    <p>在下方输入框中填写链接，每行一个：</p>
    <textarea id="links" placeholder="https://example.com/list1&#10;https://example.com/list2&#10;https://example.com/list3">${existing}</textarea>
    
    <div class="button-row">
      <button onclick="save()" id="saveBtn">保存链接</button>
      <button onclick="refreshCache()" id="refreshBtn" class="refresh-btn">立即刷新缓存</button>
      <button onclick="retryFailed()" id="retryBtn" class="retry-btn">重试分析失败</button>
      <div id="status" class="status"></div>
    </div>
    
    <div class="tips">
      <h3>使用说明</h3>
      <ul>
        <li>每行只能填写一个链接</li>
        <li>以 <code>#</code> 开头的行会被视为注释，跳过不处理</li>
        <li>访问 <code>/fetch</code> 时，程序会优先返回缓存内容，响应速度极快</li>
        <li>获取到的内容会自动删除每行原有的 <code>#</code> 后的文字（包括 <code>#</code> 本身）</li>
        <li>多个链接返回的<strong>相同内容会自动去重</strong>，保留首次出现的顺序</li>
        <li><strong>新增：</strong>自动识别每行中的 IP 地址，并在末尾追加 <code>#国家简拼国家名</code> 的地区信息</li>
        <li><span class="badge badge-new">NEW</span> <strong>后台异步处理：</strong>时间触发器触发后，先快速返回带 <code>#未处理</code> 标记的结果，之后在后台逐个分析 IP 归属地，成功替换为实际地区，失败标记 <code>#分析失败</code></li>
        <li><span class="badge badge-new">NEW</span> <strong>实时进度：</strong>后台处理期间，外部请求直接返回当前处理进度（已处理 / 未处理 / 分析失败）</li>
        <li><span class=\"badge badge-new\">NEW</span> <strong>重试失败：</strong>如果某些 IP 分析失败，可点击「重试分析失败」按钮，将这些 IP 重新标记为 <code>#未处理</code> 并再次进入分析流程</li>
        <li>示例输出：<code>192.168.1.1:8080#CN中国</code>、<code>1.2.3.4:443#未处理</code>、<code>5.6.7.8:80#分析失败</code></li>
      </ul>
    </div>
  </div>
  
  <script>
    async function save() {
      const btn = document.getElementById('saveBtn');
      const status = document.getElementById('status');
      const links = document.getElementById('links').value;
      
      btn.disabled = true;
      status.style.display = 'none';
      
      try {
        const res = await fetch('/save', {
          method: 'POST',
          headers: {'Content-Type': 'text/plain'},
          body: links
        });
        
        if (res.ok) {
          status.className = 'status success';
          status.textContent = '✓ 保存成功，缓存已自动刷新（后台开始处理 IP）';
        } else {
          const text = await res.text();
          status.className = 'status error';
          status.textContent = '✗ 保存失败: ' + text;
        }
      } catch (e) {
        status.className = 'status error';
        status.textContent = '✗ 错误: ' + e.message;
      }
      
      status.style.display = 'block';
      btn.disabled = false;
    }

    async function refreshCache() {
      const btn = document.getElementById('refreshBtn');
      const status = document.getElementById('status');
      
      btn.disabled = true;
      status.style.display = 'none';
      status.textContent = '刷新中...';
      status.className = 'status';
      status.style.display = 'block';
      
      try {
        const res = await fetch('/fetch');
        if (res.ok) {
          status.className = 'status success';
          status.textContent = '✓ 缓存刷新成功（后台开始处理 IP）';
        } else {
          const text = await res.text();
          status.className = 'status error';
          status.textContent = '✗ 刷新失败: ' + text;
        }
      } catch (e) {
        status.className = 'status error';
        status.textContent = '✗ 错误: ' + e.message;
      }
      
      btn.disabled = false;
    }
    async function retryFailed() {
      const btn = document.getElementById('retryBtn');
      const status = document.getElementById('status');
      
      btn.disabled = true;
      status.style.display = 'none';
      status.textContent = '重试中...';
      status.className = 'status';
      status.style.display = 'block';
      
      try {
        const res = await fetch('/retry-failed', { method: 'POST' });
        const text = await res.text();
        if (res.ok) {
          status.className = 'status success';
          status.textContent = '✓ ' + text;
        } else {
          status.className = 'status error';
          status.textContent = '✗ 重试失败: ' + text;
        }
      } catch (e) {
        status.className = 'status error';
        status.textContent = '✗ 错误: ' + e.message;
      }
      
      btn.disabled = false;
    }
  </script>
</body>
</html>`;
  
  return new Response(html, {
    headers: { 'Content-Type': 'text/html; charset=utf-8' }
  });
}
