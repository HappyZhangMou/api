import { connect } from 'cloudflare:sockets';

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
      if (event.cron === AGGREGATE_CRON) {
        // 每 3 小时：拉取上游并增量合并（保留已处理结果），后台处理一批 IP
        console.log('[Scheduled] Aggregation started at', new Date().toISOString());
        await aggregateLinks(env);
        console.log('[Scheduled] Aggregation completed.');
      } else {
        // 每分钟：仅增量处理待处理节点，不重新拉取上游
        await processIpQueue(env);
      }
    } catch (e) {
      console.error('[Scheduled] failed:', e.message);
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

  if (url.pathname === '/refresh' && request.method === 'POST') {
    return handleRefresh(env, ctx, corsHeaders);
  }

  if (url.pathname === '/process-now' && request.method === 'POST') {
    return handleProcessNow(env, ctx, corsHeaders);
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
    
    // 保存后自动刷新缓存；IP 队列由每分钟 Cron 独立处理。
    try {
      await aggregateLinks(env);
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

// 并发受限的 map：保持结果顺序，避免一次性打满连接数
async function mapWithConcurrency(items, limit, fn) {
  const results = new Array(items.length);
  let next = 0;
  const workers = Array.from({ length: Math.min(limit, items.length) }, async () => {
    while (true) {
      const index = next++;
      if (index >= items.length) return;
      results[index] = await fn(items[index], index);
    }
  });
  await Promise.all(workers);
  return results;
}

async function aggregateLinks(env) {
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

  // 1. 并行拉取上游链接（受并发与子请求预算限制）
  const fetchTargets = urls.slice(0, AGGREGATE_FETCH_BUDGET);
  if (urls.length > fetchTargets.length) {
    errors.push(`# 已跳过 ${urls.length - fetchTargets.length} 个上游链接（单次拉取预算 ${AGGREGATE_FETCH_BUDGET}）`);
  }

  const fetched = await mapWithConcurrency(fetchTargets, AGGREGATE_CONCURRENCY, async url => {
    try {
      const response = await fetchWithTimeout(url, {
        headers: {
          'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        },
        cf: { cacheTtl: 60 }
      }, 8000);

      if (!response.ok) {
        return { error: `# Error: HTTP ${response.status} for ${url}` };
      }
      return { text: await response.text() };
    } catch (e) {
      return { error: `# Error fetching ${url}: ${e.message}` };
    }
  });

  for (const item of fetched) {
    if (item.error) {
      errors.push(item.error);
      continue;
    }
    successCount++;
    for (const line of item.text.split('\n')) {
      const idx = line.indexOf('#');
      const processed = idx >= 0 ? line.substring(0, idx).trim() : line.trim();

      if (processed && !seen.has(processed)) {
        seen.add(processed);
        allLines.push(processed);
      }
    }
  }

  if (successCount === 0) {
    const errorOutput = errors.join('\n') || 'All links failed';
    throw new Error(errorOutput);
  }

  const digest = await crypto.subtle.digest('SHA-256', new TextEncoder().encode(`geo-v2\n${allLines.join('\n')}`));
  const upstreamHash = [...new Uint8Array(digest)].map(byte => byte.toString(16).padStart(2, '0')).join('');
  const [oldCached, oldHash] = await Promise.all([
    env.LINKS_KV.get('cached_aggregate'),
    env.LINKS_KV.get('upstream_hash')
  ]);
  if (oldCached && oldHash === upstreamHash) return oldCached;

  // 上游内容有变化时，所有节点重新入队，包括已存在测速结果的 IP。
  const markedLines = allLines.map(line => extractIp(line) ? `${line}#未处理` : line);

  // 3. 组装初始输出并立即写入缓存
  let output = markedLines.join('\n');
  if (errors.length > 0) {
    output += '\n\n' + errors.join('\n');
  }

  // 立即写入缓存，让外部请求可以获取到（带 #未处理 标记）
  await writeCachedAggregate(env, output);
  await env.LINKS_KV.put('upstream_hash', upstreamHash);

  return output;
}

// 辅助：统一写入缓存。只写一个 key，减少 KV 每日写入次数。
async function writeCachedAggregate(env, output, ttl = 60 * 60 * 3) {
  try {
    await env.LINKS_KV.put('cached_aggregate', output, { expirationTtl: ttl });
    return Date.now();
  } catch (e) {
    console.error('[Cache] writeCachedAggregate failed:', e.message);
    throw e;
  }
}

// 简易 KV 锁：防止多个入口并发处理同一批 IP 导致覆盖写入。
// 带 TTL，实例异常退出也会自动过期，避免死锁。
async function tryAcquireLock(env, ttl = 120) {
  try {
    const existing = await env.LINKS_KV.get('processing_lock');
    if (existing === '1') return false;
    await env.LINKS_KV.put('processing_lock', '1', { expirationTtl: ttl });
    return true;
  } catch (e) {
    console.error('[Lock] tryAcquireLock failed:', e.message);
    return false;
  }
}

async function releaseLock(env) {
  try {
    await env.LINKS_KV.delete('processing_lock');
  } catch (e) {
    console.error('[Lock] releaseLock failed:', e.message);
  }
}

// ==================== 后台 IP 处理队列 ====================

async function processIpQueue(env) {
  try {
    // 先读一次缓存：没有待处理项就直接返回，避免无谓的加锁与 KV 写入
    const preview = await env.LINKS_KV.get('cached_aggregate');
    if (!preview) {
      return;
    }
    const hasPending = /#未处理|#分析失败-待重试/.test(preview);
    const previewSorted = sortLinesByLatency(preview.split('\n')).join('\n');
    if (!hasPending && previewSorted === preview) return;

    // 抢锁：防止 cron / 手动 / retry 等多个入口并发处理、互相覆盖
    if (!(await tryAcquireLock(env))) {
      console.log('[Queue] Another run is in progress, skip.');
      return;
    }

    let processedCount = 0;
    let failedCount = 0;
    let deletedCount = 0;
    let skippedCount = 0;
    let deferredCount = 0;
    let speedFailedCount = 0;

    try {
      const current = await env.LINKS_KV.get('cached_aggregate');
      if (!current) return;
      const lines = current.split('\n');

      const pendingItems = [];
      let changed = false;

      // 收集本轮待处理项
      for (let i = 0; i < lines.length && pendingItems.length < MAX_PER_RUN; i++) {
        const line = lines[i];
        let contentWithoutMark = null;

        if (line.endsWith('#未处理')) {
          contentWithoutMark = line.slice(0, -4);
        } else {
          const retryMatch = line.match(/^(.*)#分析失败-待重试(?:@(\d+))?$/);
          if (retryMatch) {
            const ts = retryMatch[2] ? parseInt(retryMatch[2], 10) : 0;
            if (!ts || ts <= Date.now() - RETRY_DELAY_MS) {
              contentWithoutMark = retryMatch[1];
            } else {
              continue; // 未到重试时间
            }
          } else if (line.endsWith('#UN未知')) {
            contentWithoutMark = line.slice(0, -5);
          }
        }

        if (contentWithoutMark !== null) {
          const ip = extractIp(contentWithoutMark);
          if (ip) {
            pendingItems.push({ index: i, line: contentWithoutMark, ip });
          } else {
            lines[i] = contentWithoutMark;
            changed = true;
            skippedCount++;
          }
        }
      }

      if (pendingItems.length === 0) {
        // 即使没有待处理项，也整理已经完成的延迟结果。
        const sortedLines = sortLinesByLatency(lines);
        const output = sortedLines.join('\n');
        if (changed || output !== current) {
          await writeCachedAggregate(env, output);
        }
        return;
      }

      // 按干净 IP 去重分组：同一 IP 只查一次，结果应用到所有行
      const byIp = new Map();
      for (const item of pendingItems) {
        const clean = getCleanIp(item.ip);
        if (!byIp.has(clean)) byIp.set(clean, []);
        byIp.get(clean).push(item);
      }
      const uniqueIps = [...byIp.keys()];

      const { resolved, unresolved, status: batchStatus } = await resolveIpLocations(uniqueIps);
      if (batchStatus !== 'ok') {
        console.warn(`[IP API] batch lookup status: ${batchStatus}; checking unresolved IPs with fallback APIs.`);
      }
      if (unresolved.size > 0) {
        console.log(`[IP API] ${unresolved.size} IP(s) need fallback comparison.`);
      }

      const fallbackIps = [...unresolved.keys()];
      const batchSubrequestCount = Math.ceil(uniqueIps.length / BATCH_MAX_IPS) * BATCH_SUBREQUESTS_PER_CHUNK;
      const fallbackBudget = Math.max(0, Math.floor(
        (SUBREQUEST_BUDGET - batchSubrequestCount) / FALLBACK_SUBREQUESTS_PER_IP
      ));
      const fallbackTargets = fallbackIps.slice(0, fallbackBudget);
      const attemptedFallbackIps = new Set(fallbackTargets);
      const fallbackResults = await mapWithConcurrency(fallbackTargets, FALLBACK_CONCURRENCY, async ip => {
        const candidates = await getIpLocationCandidates(ip);
        const result = selectCountryConsensus([...(unresolved.get(ip) || []), ...candidates]);
        return { ip, result };
      });
      for (const { ip, result } of fallbackResults) {
        if (result) resolved.set(ip, result);
      }

      const itemResults = await mapWithConcurrency(pendingItems, SPEED_TEST_CONCURRENCY, async item => {
        const clean = getCleanIp(item.ip);
        const result = resolved.get(clean);
        if (result === undefined) {
          if (unresolved.has(clean) && !attemptedFallbackIps.has(clean)) {
            return { line: `${item.line}#未处理`, deferred: true };
          }
          return { line: `${item.line}#分析失败-待重试@${Date.now()}`, failed: true };
        }
        if (result === CLOUDFLARE_MARK) {
          return { line: `${item.line}#Cloudflare`, cloudflare: true };
        }
        if (result.startsWith('CN')) return { line: null, deleted: true };

        const speed = await measureIpConnection(item.line);
        if (speed.error) {
          return { line: `${item.line}#${result}|连接失败`, speedFailed: true };
        }
        return { line: `${item.line}#${result}|${speed.latencyMs}ms` };
      });

      for (let i = 0; i < pendingItems.length; i++) {
        lines[pendingItems[i].index] = itemResults[i].line;
        if (itemResults[i].failed) failedCount++;
        else if (itemResults[i].deleted) deletedCount++;
        else if (itemResults[i].deferred) deferredCount++;
        else {
          processedCount++;
          if (itemResults[i].speedFailed) speedFailedCount++;
        }
      }

      // 本轮只写回一次 KV（过滤掉已删除的行）
      const filteredLines = lines.filter(line => line !== null);
      await writeCachedAggregate(env, sortLinesByLatency(filteredLines).join('\n'));

      console.log(`[Queue] Run done. Batch: ${batchStatus}, Processed: ${processedCount}, Deferred: ${deferredCount}, Speed failed: ${speedFailedCount || 0}, Failed(retry later): ${failedCount}, Deleted: ${deletedCount}, Skipped: ${skippedCount}`);
    } finally {
      await releaseLock(env);
    }
  } catch (e) {
    console.error('[Queue] Error in processIpQueue:', e.message);
  }
}

function sortLinesByLatency(lines) {
  return lines
    .map((line, index) => ({
      line,
      index,
      latency: Number((line.match(/\|(\d+(?:\.\d+)?)ms$/) || [])[1])
    }))
    .sort((left, right) => {
      const leftMeasured = Number.isFinite(left.latency);
      const rightMeasured = Number.isFinite(right.latency);
      if (leftMeasured && rightMeasured) return left.latency - right.latency || left.index - right.index;
      if (leftMeasured) return -1;
      if (rightMeasured) return 1;
      return left.index - right.index;
    })
    .map(item => item.line);
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
      // 支持多种失败标记：#分析失败、#分析失败-待重试 或 带时间戳的 #分析失败-待重试@<ts>
      const match = line.match(/^(.*)(#分析失败(?:-待重试(?:@\d+)?)?|#[^#]*\|连接失败)$/);
      if (match) {
        retryCount++;
        return match[1] + '#未处理';
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

async function measureIpConnection(line) {
  const bracketed = line.match(/\[([0-9a-fA-F:]+)\](?::(\d+))?/);
  const ipv4 = line.match(/\b(\d{1,3}(?:\.\d{1,3}){3})(?::(\d+))?\b/);
  const endpoint = bracketed || ipv4;
  if (!endpoint) return { error: '未找到可测速 IP' };

  const hostname = endpoint[1];
  const port = Number(endpoint[2] || 443);
  if (!Number.isInteger(port) || port < 1 || port > 65535) return { error: '端口无效' };

  let socket;
  let timeout;
  const startedAt = Date.now();
  try {
    socket = connect({ hostname, port });
    await Promise.race([
      socket.opened,
      new Promise((_, reject) => {
        timeout = setTimeout(() => reject(new Error('连接超时')), 3000);
      })
    ]);
    return { latencyMs: Math.max(1, Date.now() - startedAt) };
  } catch (error) {
    return { error: error.message || '连接失败' };
  } finally {
    clearTimeout(timeout);
    if (socket) await socket.close().catch(() => {});
  }
}

// 管理端：强制触发后台处理（用于排查和即时处理）
async function handleRefresh(env, ctx, corsHeaders) {
  try {
    const previousHash = await env.LINKS_KV.get('upstream_hash');
    await aggregateLinks(env);
    const currentHash = await env.LINKS_KV.get('upstream_hash');
    const changed = previousHash !== currentHash;
    const message = changed
      ? '上游刷新完成，内容有变化，已重新排队测速。'
      : '上游刷新完成，内容没有变化，无需重新测速。';
    return new Response(message, {
      status: 200,
      headers: { ...corsHeaders, 'Content-Type': 'text/plain; charset=utf-8' }
    });
  } catch (e) {
    return new Response('Refresh Error: ' + e.message, {
      status: 502,
      headers: { ...corsHeaders, 'Content-Type': 'text/plain; charset=utf-8' }
    });
  }
}

async function handleProcessNow(env, ctx, corsHeaders) {
  try {
    // 启动后台处理但不要阻塞响应
    if (ctx) ctx.waitUntil(processIpQueue(env));
    return new Response('Process started', { status: 200, headers: { ...corsHeaders, 'Content-Type': 'text/plain; charset=utf-8' } });
  } catch (e) {
    return new Response('Process Error: ' + e.message, { status: 500, headers: { ...corsHeaders, 'Content-Type': 'text/plain; charset=utf-8' } });
  }
}

// ==================== /fetch 端点（直接返回当前缓存） ====================

async function handleFetch(env, corsHeaders, ctx) {
  // 始终直接返回当前缓存内容（可能包含 #未处理、已完成分析、#分析失败 的混合状态）
  try {
    const cached = await env.LINKS_KV.get('cached_aggregate');
    if (cached !== null && cached !== undefined) {
      // 处理进度由 cron 定时任务或 /process-now、/retry-failed 推进；
      // 这里不再每次访问都触发后台任务，避免并发叠加与子请求/写入浪费
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
    const output = await aggregateLinks(env);
    // aggregateLinks 会写入缓存；IP 队列由每分钟 Cron 独立处理。
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

// API 退避（当某个 API 返回 429 时，记录退避到期时间）
const apiBackoff = new Map();
const API_BACKOFF_MS = 60 * 1000; // 1 分钟退避

// 主 API 速率限制（轻量、本实例内）：每分钟可保留调用数
const apiUsage = new Map();
const API_WINDOW_MS = 60 * 1000;
const API_PRIMARY_LIMIT = 30; // 主接口本地限速（保守）；额度用完后自动切备用接口

const BATCH_MAX_IPS = 100;
// 每轮批量归属查询上限；补查和测速分别由各自的限流控制。
const MAX_PER_RUN = BATCH_MAX_IPS;
const FALLBACK_CONCURRENCY = 3;
const SPEED_TEST_CONCURRENCY = 6;
// 单次 invocation 的外部子请求预算（Free 版上限 50，预留 6 次给 KV/杂项）
const SUBREQUEST_BUDGET = 44;
// 每个批次的批量查询固定消耗：ip-api + countries.dev 各 1 次
const BATCH_SUBREQUESTS_PER_CHUNK = 2;
// 单个 IP 降级查询最多消耗：uapis.cn + ipwho.is 各 1 次
const FALLBACK_SUBREQUESTS_PER_IP = 2;
// 上游拉取并发（与 6 个并发连接上限对齐）
const AGGREGATE_CONCURRENCY = 6;
// 上游聚合与 IP 队列分属不同 invocation，各自使用独立子请求预算。
const AGGREGATE_FETCH_BUDGET = SUBREQUEST_BUDGET;
// ip-api 判定为 Cloudflare(AS13335) 的 IP 用此标记，不再当普通地区节点
const CLOUDFLARE_MARK = 'CLOUDFLARE';
const RETRY_DELAY_MS = 5 * 60 * 1000; // 失败项 5 分钟后可再次处理

// 批量 IP 归属地接口（ip-api.com 免费批量，POST，每次最多 100 个 IP）
const BATCH_API_URL = 'http://ip-api.com/batch';
const BATCH_API_URL_SECONDARY = 'https://countries.dev/ip?fields=name';
// 定时触发：区分「重新拉取上游」与「增量处理 IP」
const AGGREGATE_CRON = '0 */3 * * *'; // 每 3 小时拉取一次上游订阅
const PROCESS_CRON = '* * * * *'; // 每分钟处理一批 IP

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
  'PS': '巴勒斯坦', 'KW': '科威特', 'QA': '卡塔尔', 'BH': '巴林', 'OM': '阿曼'
};

// 通用 fetch 带超时封装
function fetchWithTimeout(resource, options = {}, timeoutMs = 5000) {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error('timeout')), timeoutMs);
    fetch(resource, options).then(resp => {
      clearTimeout(timer);
      resolve(resp);
    }, err => {
      clearTimeout(timer);
      reject(err);
    });
  });
}

// 批量查询 IP 归属地（ip-api.com 免费批量接口，一次最多 100 个 IP）
// 返回 { results: Map<ip, 结果字符串>, blocked: Set<Cloudflare IP>, status: 'ok' | 'rate_limited' | 'error' }
async function getIpLocationBatch(cleanIps) {
  try {
    const resp = await fetchWithTimeout(BATCH_API_URL, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(cleanIps)
    }, 5000);

    if (!resp.ok) {
      if (resp.status === 429) {
        console.log('[IP API] batch rate-limited (HTTP 429)');
        return { results: new Map(), blocked: new Set(), status: 'rate_limited' };
      }
      return { results: new Map(), blocked: new Set(), status: 'error' };
    }

    const data = await resp.json();
    const results = new Map();
    const blocked = new Set();
    if (Array.isArray(data)) {
      for (let i = 0; i < data.length; i++) {
        const item = data[i];
        const ip = cleanIps[i];
        if (!item || item.status !== 'success' || !ip) continue;
        const providerInfo = `${item.as || ''} ${item.isp || ''} ${item.org || ''}`;
        if (/\bAS13335\b/i.test(providerInfo) || /cloudflare/i.test(providerInfo)) {
          blocked.add(ip);
          continue;
        }
        const code = String(item.countryCode || '').toUpperCase();
        const name = countryCodeMap[code] || item.country || '未知';
        results.set(ip, `${code}${name}`);
      }
    }
    return { results, blocked, status: 'ok' };
  } catch (e) {
    console.error('[IP API] batch query failed:', e.message);
    return { results: new Map(), blocked: new Set(), status: 'error' };
  }
}

// 第二个批量接口（countries.dev，免 key，最多 100 个 IP）
async function getIpLocationBatchSecondary(cleanIps) {
  try {
    const resp = await fetchWithTimeout(BATCH_API_URL_SECONDARY, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(cleanIps)
    }, 5000);

    if (!resp.ok) {
      if (resp.status === 429) {
        console.log('[IP API] secondary batch rate-limited (HTTP 429)');
        return { results: new Map(), status: 'rate_limited' };
      }
      return { results: new Map(), status: 'error' };
    }

    const data = await resp.json();
    const results = new Map();
    if (Array.isArray(data)) {
      for (let i = 0; i < data.length; i++) {
        const item = data[i];
        const ip = cleanIps[i];
        if (!item || !item.countryCode || !ip) continue;
        const code = String(item.countryCode).toUpperCase();
        const name = countryCodeMap[code] || (item.country && item.country.name) || '未知';
        results.set(ip, `${code}${name}`);
      }
    }
    return { results, status: 'ok' };
  } catch (e) {
    console.error('[IP API] secondary batch query failed:', e.message);
    return { results: new Map(), status: 'error' };
  }
}

// 并行查询两个批量接口；只有国家代码一致时直接采纳，否则保留候选交给限速接口仲裁。
async function resolveIpLocations(uniqueIps) {
  const resolved = new Map();
  const unresolved = new Map();
  let status = 'ok';

  for (let i = 0; i < uniqueIps.length; i += BATCH_MAX_IPS) {
    const batch = uniqueIps.slice(i, i + BATCH_MAX_IPS);
    const [primary, secondary] = await Promise.all([
      getIpLocationBatch(batch),
      getIpLocationBatchSecondary(batch)
    ]);

    for (const ip of batch) {
      if (primary.blocked.has(ip)) {
        resolved.set(ip, CLOUDFLARE_MARK);
        continue;
      }

      const primaryResult = primary.results.get(ip);
      const secondaryResult = secondary.results.get(ip);
      const candidates = [primaryResult, secondaryResult]
        .filter(result => result && getCountryResultKey(result));
      const primaryKey = getCountryResultKey(primaryResult);
      const secondaryKey = getCountryResultKey(secondaryResult);
      if (primaryKey && primaryKey === secondaryKey) {
        resolved.set(ip, primaryResult);
      } else {
        unresolved.set(ip, candidates);
      }
    }

    if (primary.results.size === 0 && secondary.results.size === 0 &&
        primary.blocked.size === 0 &&
        primary.status !== 'ok' && secondary.status !== 'ok') {
      status = primary.status === 'rate_limited' || secondary.status === 'rate_limited'
        ? 'rate_limited'
        : 'error';
      break;
    }
  }

  return { resolved, unresolved, status };
}

// 查询所有当前可用的限速接口，结果由调用方与批量接口候选交叉仲裁。
async function getIpLocationCandidates(ip) {
  const cleanIp = getCleanIp(ip);
  if (!cleanIp) return [];

  const apis = [
    {
      name: 'uapis.cn',
      url: `https://uapis.cn/api/v1/network/ipinfo?ip=${cleanIp}`,
      extract: data => (data && data.region ? data.region.trim() : null),
      timeout: 3000
    },
    {
      name: 'ipwho.is',
      url: `https://ipwho.is/${cleanIp}?output=json`,
      extract: data => (data ? (data.country_code || data.country || data.country_code2 || null) : null),
      timeout: 3000
    }
  ];

  const results = await Promise.all(apis.map(async api => {
    if (isApiBackedOff(api.name)) return null;
    if (api.name === 'uapis.cn' && !consumePrimaryQuota(api.name)) return null;
    const outcome = await queryApi(cleanIp, api);
    return outcome.result;
  }));
  return results.filter(result => result && getCountryResultKey(result));
}

function getCountryResultKey(result) {
  const match = String(result || '').trim().match(/^([A-Z]{2})/i);
  if (!match) return null;
  const code = match[1].toUpperCase();
  return code === 'UN' ? null : code;
}

function selectCountryConsensus(results) {
  const votes = new Map();
  for (const result of results) {
    const code = getCountryResultKey(result);
    if (!code) continue;
    const vote = votes.get(code) || { count: 0, result };
    vote.count++;
    votes.set(code, vote);
  }

  const ranked = [...votes.values()].sort((left, right) => right.count - left.count);
  if (!ranked.length || ranked[0].count < 2 || ranked[0].count === ranked[1]?.count) return null;
  return ranked[0].result;
}

// 检查并消耗主接口本地额度（滚动 1 分钟窗口）
function consumePrimaryQuota(apiName) {
  const now = Date.now();
  const usage = apiUsage.get(apiName) || { windowStart: now, count: 0 };
  if (now - usage.windowStart > API_WINDOW_MS) {
    usage.windowStart = now;
    usage.count = 0;
  }
  if (usage.count >= API_PRIMARY_LIMIT) {
    return false; // 本轮额度用完，切备用
  }
  usage.count++;
  apiUsage.set(apiName, usage);
  return true;
}

// 调用单个 IP 接口，返回 { result, rateLimited }
async function queryApi(cleanIp, api) {
  try {
    const resp = await fetchWithTimeout(api.url, { cf: { cacheTtl: 86400 } }, api.timeout);
    if (!resp.ok) {
      if (resp.status === 429) {
        apiBackoff.set(api.name, Date.now() + API_BACKOFF_MS);
        console.log(`[IP API] ${api.name} rate-limited (HTTP 429)`);
        return { result: null, rateLimited: true };
      }
      return { result: null, rateLimited: false };
    }

    const data = await resp.json().catch(() => null);
    const extracted = api.extract(data);
    if (extracted) {
      return { result: toCountryResult(api.name, extracted), rateLimited: false };
    }
    return { result: null, rateLimited: false };
  } catch (e) {
    const msg = (e && e.message) ? e.message : '';
    if (/429|Too Many Requests/i.test(msg)) {
      apiBackoff.set(api.name, Date.now() + API_BACKOFF_MS);
      console.log(`[IP API] ${api.name} rate-limited (${msg})`);
      return { result: null, rateLimited: true };
    }
    console.error(`[IP API] ${api.name} error:`, msg);
    return { result: null, rateLimited: false };
  }
}

function isApiBackedOff(apiName) {
  const until = apiBackoff.get(apiName) || 0;
  return until > Date.now();
}

// 将接口返回的原始地区信息转换为 `国家代码 + 中文国家名` 格式
function toCountryResult(apiName, extracted) {
  if (apiName === 'uapis.cn') {
    const region = String(extracted || '').trim();
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
    return `${code}${countryName}`;
  }

  const extractedStr = String(extracted || '').trim();
  const code = extractedStr.length === 2 ? extractedStr.toUpperCase() : '';
  const countryName = countryCodeMap[code] || (extractedStr || '未知');
  return `${code}${countryName}`;
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
    let connectionFailed = 0;
    
    for (const line of allLines) {
      const trimmed = line.trim();
      if (!trimmed || trimmed.startsWith('#')) continue;
      totalLines++;
      if (trimmed.endsWith('#未处理')) unprocessed++;
      else if (/#分析失败(?:-待重试(?:@\d+)?)?$/.test(trimmed)) {
        if (trimmed.endsWith('#分析失败')) failed++;
        else failedRetry++;
      } else if (trimmed.endsWith('|连接失败')) connectionFailed++;
    }
    
    const processed = totalLines - unprocessed - failed - failedRetry - connectionFailed;
    cacheInfo = `已缓存（约 ${totalLines} 条记录，已处理 ${processed}，未处理 ${unprocessed}，待重试 ${failedRetry}，地区失败 ${failed}，连接失败 ${connectionFailed}）`;
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
      /* 每3小时触发聚合（需在 wrangler.toml 配置 crons = ["0 */3 * * *"]） */
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
      <strong>API 端点：</strong>请访问 <code>/fetch</code> 获取汇总后的内容<br>
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
      <button onclick="refreshCache()" id="refreshBtn" class="refresh-btn">立即拉取并刷新</button>
      <button onclick="processNow()" id="processNowBtn" class="refresh-btn">立即处理</button>
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
        <li><strong>IP 连接延迟：</strong>上游内容变化后会重新测试 IP:端口的 TCP 建连时间，结果格式为 <code>#地区|42ms</code>；Cloudflare 出站策略可能拒绝优选 IP 连接</li>
        <li>示例输出：<code>1.2.3.4:443#JP日本|42ms</code>；连接失败会显示 <code>|连接失败</code>，此值是 Worker 到 IP 的 TCP 延迟，不是下载速度</li>
        <li><span class="badge badge-new">NEW</span> <strong>后台异步处理：</strong>时间触发器触发后，先快速返回带 <code>#未处理</code> 标记的结果，之后在后台逐个分析 IP 归属地，成功替换为实际地区，失败标记 <code>#分析失败</code></li>
        <li><span class="badge badge-new">NEW</span> <strong>实时进度：</strong>后台处理期间，外部请求直接返回当前处理进度（已处理 / 未处理 / 分析失败）</li>
        <li><span class="badge badge-new">NEW</span> <strong>重试失败：</strong>如果某些 IP 分析失败，可点击「重试分析失败」按钮，将这些 IP 重新标记为 <code>#未处理</code> 并再次进入分析流程</li>
        <li>示例输出：<code>1.2.3.4:443#JP日本|42ms</code>、<code>1.2.3.4:443#未处理</code>、<code>5.6.7.8:80#连接失败</code></li>
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
        const res = await fetch('/refresh', { method: 'POST' });
        const text = await res.text();
        if (res.ok) {
          status.className = 'status success';
          status.textContent = '✓ ' + text;
        } else {
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

    async function processNow() {
      const btn = document.getElementById('processNowBtn');
      const status = document.getElementById('status');

      btn.disabled = true;
      status.style.display = 'none';
      status.textContent = '请求处理中...';
      status.className = 'status';
      status.style.display = 'block';

      try {
        const res = await fetch('/process-now', { method: 'POST' });
        const text = await res.text();
        if (res.ok) {
          status.className = 'status success';
          status.textContent = '✓ ' + text;
        } else {
          status.className = 'status error';
          status.textContent = '✗ 处理失败: ' + text;
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
