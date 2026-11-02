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
        // 处理节点：cron 触发允许 15 分钟，这里用满 10 分钟以提高单轮吞吐。
        await processIpQueue(env, CRON_RUN_BUDGET_MS);
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

  if (url.pathname === '/reprocess' && request.method === 'POST') {
    return handleReprocess(env, ctx, corsHeaders);
  }

  if (url.pathname === '/reset' && request.method === 'POST') {
    return handleReset(env, ctx, corsHeaders);
  }

  if (url.pathname === '/probe') {
    return handleProbe(url);
  }

  if (url.pathname === '/geo') {
    return handleGeoProbe(url);
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

  // IP 与优选域名都要测速：只要能被解析成测速端点，就进入待处理队列。
  const markedLines = allLines.map(line => parseEndpoint(line) ? `${line}#未处理` : line);

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

// 同一 key 每秒最多写 1 次；这里强制最小间隔，避免手动连点或多个入口并发写导致 429。
// 注意：不要用模块级变量统计每日写入量——Workers 每次 invocation 可能落在新 isolate，
// 变量会重置，这类"预算"既不准也拦不住问题，只保留必要的节流。
const MIN_WRITE_INTERVAL_MS = 1200;
let lastWriteAt = 0;
let pendingWrite = Promise.resolve();

async function writeCachedAggregate(env, output, ttl = 60 * 60 * 3) {
  // 串行化 + 最小间隔，满足 KV 同 key 1 次/秒的限制。
  const run = async () => {
    const wait = MIN_WRITE_INTERVAL_MS - (Date.now() - lastWriteAt);
    if (wait > 0) await new Promise(resolve => setTimeout(resolve, wait));
    try {
      await env.LINKS_KV.put('cached_aggregate', output, { expirationTtl: ttl });
      lastWriteAt = Date.now();
      return lastWriteAt;
    } catch (e) {
      console.error('[Cache] writeCachedAggregate failed:', e.message);
      throw e;
    }
  };

  const task = pendingWrite.then(run, run);
  pendingWrite = task.catch(() => {});
  return task;
}

// 实例内处理锁：替代 KV 锁。KV 锁每轮要 2 次写入（put+delete），
// 在 Free 计划每天仅 1000 次写入的额度下不可接受。
// 该锁只在当前 isolate 内有效，配合每分钟 cron 已足够避免同一实例内的重复处理；
// 即使多实例同时处理，写回是整份覆盖且结果幂等，最坏情况只是重复测速。
let localProcessing = false;

function tryAcquireLock() {
  if (localProcessing) return false;
  localProcessing = true;
  return true;
}

function releaseLock() {
  localProcessing = false;
}

// ==================== 后台 IP 处理队列 ====================

async function processIpQueue(env, timeBudgetMs = HTTP_RUN_BUDGET_MS) {
  try {
    // 先读一次缓存：没有待处理项就直接返回，避免无谓的加锁与 KV 写入
    const preview = await env.LINKS_KV.get('cached_aggregate');
    if (!preview) {
      return;
    }
    const hasPending = preview.includes('#未处理');
    const previewSorted = sortLinesByLatency(preview.split('\n')).join('\n');
    if (!hasPending && previewSorted === preview) return;

    // 实例内锁：防止同一实例的 cron / 手动入口并发处理
    if (!tryAcquireLock()) {
      console.log('[Queue] Another run is in progress, skip.');
      return;
    }
    let processedCount = 0;
    let deferredCount = 0;
    let deletedCount = 0;
    let skippedCount = 0;

    try {
      const current = await env.LINKS_KV.get('cached_aggregate');
      if (!current) return;
      const lines = current.split('\n');

      const pendingItems = [];
      let changed = false;

      // 收集本轮待处理项：只扫一遍，遇到 #未处理 即收集，凑够上限就停。
      for (let i = 0; i < lines.length; i++) {
        if (pendingItems.length >= MAX_PER_RUN) break;

        const line = lines[i];
        if (!line.endsWith('#未处理')) continue;

        const contentWithoutMark = line.slice(0, -4);
        const endpoint = parseEndpoint(contentWithoutMark);
        if (!endpoint) {
          lines[i] = contentWithoutMark;
          changed = true;
          skippedCount++;
          continue;
        }
        pendingItems.push({
          index: i,
          line: contentWithoutMark,
          ip: extractIp(contentWithoutMark),
          endpoint,
          key: `${endpoint.hostname}:${endpoint.port}`
        });
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

      // 按干净 IP 去重分组：同一 IP 只查一次，结果应用到所有行；域名行不参与归属地查询
      const byIp = new Map();
      for (const item of pendingItems) {
        if (!item.ip) continue;
        const clean = getCleanIp(item.ip);
        if (!byIp.has(clean)) byIp.set(clean, []);
        byIp.get(clean).push(item);
      }
      const uniqueIps = [...byIp.keys()];

      let resolved = new Map();
      let unresolved = new Map();
      let batchStatus = 'ok';
      if (uniqueIps.length > 0) {
        const lookup = await resolveIpLocations(uniqueIps);
        resolved = lookup.resolved;
        unresolved = lookup.unresolved;
        batchStatus = lookup.status;
      }
      if (batchStatus !== 'ok') {
        console.warn(`[IP API] batch lookup status: ${batchStatus}; keeping nodes pending to avoid mass deletion.`);
      }
      if (unresolved.size > 0) {
        console.log(`[IP API] ${unresolved.size} IP(s) not confirmed; kept pending for retry.`);
      }

      // 端点级去重：同一 hostname:port 只测速一次，结果共享给所有重复行。
      const speedCache = new Map();
      const startedAt = Date.now();

      async function getSpeed(key, endpoint) {
        if (speedCache.has(key)) return speedCache.get(key);
        const promise = measureEndpoint(endpoint);
        speedCache.set(key, promise);
        return promise;
      }

      function withinLatencyRange(latencyMs) {
        return latencyMs >= LATENCY_MIN_MS && latencyMs <= LATENCY_MAX_MS;
      }

      const itemResults = await mapWithConcurrency(pendingItems, SPEED_TEST_CONCURRENCY, async item => {
        // 时间预算耗尽：剩余项保留待处理，交给下一轮继续，避免 invocation 被强杀导致进度丢失。
        if (Date.now() - startedAt > timeBudgetMs) {
          return { line: `${item.line}#未处理`, deferred: true };
        }

        // 优选域名只测延迟，不查归属地，也不参与 CN 过滤
        if (!item.ip) {
          const speed = await getSpeed(item.key, item.endpoint);
          if (speed.error) return { line: null, deleted: true, reason: 'domain-speed-fail' };
          if (!withinLatencyRange(speed.latencyMs)) return { line: null, deleted: true, reason: 'domain-latency' };
          return { line: `${item.line}|${speed.latencyMs}ms` };
        }

        const clean = getCleanIp(item.ip);
        const result = resolved.get(clean);
        if (result === undefined) {
          // 归属未确认：无论是否接口故障，都先保留待处理下轮再试。
          // 之前的"直接删除"会因单轮接口波动把大量可用节点一次性清空，风险过高。
          return { line: `${item.line}#未处理`, deferred: true };
        }
        if (result === CLOUDFLARE_MARK) {
          return { line: `${item.line}#Cloudflare`, cloudflare: true };
        }
        if (result.startsWith('CN')) return { line: null, deleted: true, reason: 'cn' };

        const speed = await getSpeed(item.key, item.endpoint);
        if (speed.error) return { line: `${item.line}#未处理`, deferred: true };
        if (!withinLatencyRange(speed.latencyMs)) return { line: null, deleted: true, reason: 'latency' };
        return { line: `${item.line}#${result}|${speed.latencyMs}ms` };
      });

      const deleteReasons = new Map();
      for (let i = 0; i < pendingItems.length; i++) {
        lines[pendingItems[i].index] = itemResults[i].line;
        if (itemResults[i].deleted) {
          deletedCount++;
          const r = itemResults[i].reason || 'unknown';
          deleteReasons.set(r, (deleteReasons.get(r) || 0) + 1);
        } else if (itemResults[i].deferred) deferredCount++;
        else processedCount++;
      }

      // 本轮只写回一次 KV（过滤掉已删除的行）
      const filteredLines = lines.filter(line => line !== null);
      await writeCachedAggregate(env, sortLinesByLatency(filteredLines).join('\n'));

      const reasonText = [...deleteReasons.entries()].map(([k, v]) => `${k}=${v}`).join(', ') || 'none';
      console.log(`[Queue] Run done. Batch: ${batchStatus}, Processed: ${processedCount}, Deferred: ${deferredCount}, Deleted: ${deletedCount} [${reasonText}], Skipped: ${skippedCount}`);
    } finally {
      releaseLock();
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

async function measureEndpoint(endpoint) {
  if (!endpoint) return { error: '未找到可测速端点' };
  const { hostname, port } = endpoint;
  if (!Number.isInteger(port) || port < 1 || port > 65535) return { error: '端口无效' };

  const samples = [];
  let lastError = null;
  for (let attempt = 0; attempt < SPEED_TEST_ATTEMPTS; attempt++) {
    const sample = await measureOnce(hostname, port);
    if (sample.error) {
      lastError = sample.error;
      continue;
    }
    samples.push(sample.latencyMs);
  }

  // 抖动容忍：只要有成功样本就取其中最快值（代表可达时的真实延迟），全部失败才判定失败。
  if (samples.length === 0) return { error: lastError || '连接失败' };

  return { latencyMs: Math.max(1, Math.min(...samples)) };
}

async function measureOnce(hostname, port) {
  let socket;
  let timeout;
  const startedAt = Date.now();
  try {
    socket = connect({ hostname, port });
    await Promise.race([
      socket.opened,
      new Promise((_, reject) => {
        timeout = setTimeout(() => reject(new Error('连接超时')), CONNECT_TIMEOUT_MS);
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

// 解析测速端点：优先 IP，其次域名。域名行仅用于延迟测试，不参与归属地查询。
function parseEndpoint(line) {
  const bracketed = line.match(/\[([0-9a-fA-F:]+)\](?::(\d+))?/);
  if (bracketed && isValidIPv6(bracketed[1])) {
    return { hostname: bracketed[1], port: normalizePort(bracketed[2]) };
  }

  const ipv4 = line.match(/\b(\d{1,3}(?:\.\d{1,3}){3})(?::(\d+))?\b/);
  if (ipv4 && isValidIPv4(ipv4[1])) {
    return { hostname: ipv4[1], port: normalizePort(ipv4[2]) };
  }

  const host = line.match(/^([a-zA-Z0-9](?:[a-zA-Z0-9-]*[a-zA-Z0-9])?(?:\.[a-zA-Z0-9](?:[a-zA-Z0-9-]*[a-zA-Z0-9])?)+)(?::(\d+))?$/);
  if (host) {
    return { hostname: host[1].toLowerCase(), port: normalizePort(host[2]) };
  }

  return null;
}

function normalizePort(value) {
  const port = Number(value || 443);
  return Number.isInteger(port) && port >= 1 && port <= 65535 ? port : 0;
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

// 重新处理：丢弃所有已有归属/测速结果，把全部可测速端点重新排队。
async function handleReprocess(env, ctx, corsHeaders) {
  try {
    const cached = await env.LINKS_KV.get('cached_aggregate');
    if (!cached) {
      return new Response('No cache found', {
        status: 404,
        headers: { ...corsHeaders, 'Content-Type': 'text/plain; charset=utf-8' }
      });
    }

    let resetCount = 0;
    const updatedLines = cached.split('\n').map(line => {
      const trimmed = line.trim();
      if (!trimmed || trimmed.startsWith('#')) return line;

      // 程序追加的结果一律在第一个 # 之后（如 #地区|Nms、#未处理、#Cloudflare），整体剥离。
      const hashIndex = trimmed.indexOf('#');
      const base = (hashIndex >= 0 ? trimmed.slice(0, hashIndex) : trimmed)
        .replace(/\|[^|]*$/, '')
        .trim();

      if (!parseEndpoint(base)) return line;
      resetCount++;
      return `${base}#未处理`;
    });

    if (resetCount === 0) {
      return new Response('没有可重新处理的节点', {
        headers: { ...corsHeaders, 'Content-Type': 'text/plain; charset=utf-8' }
      });
    }

    await writeCachedAggregate(env, updatedLines.join('\n'));
    if (ctx) ctx.waitUntil(processIpQueue(env));

    return new Response(`已将 ${resetCount} 个节点重置为未处理，后台开始重新处理`, {
      headers: { ...corsHeaders, 'Content-Type': 'text/plain; charset=utf-8' }
    });
  } catch (e) {
    return new Response('Reprocess Error: ' + e.message, {
      status: 500,
      headers: { ...corsHeaders, 'Content-Type': 'text/plain; charset=utf-8' }
    });
  }
}

// 强制重建：清空 upstream_hash 后重新拉取上游，把所有节点重新入队。
// 用于节点被大量删除后恢复列表（hash 未变时 /refresh 会直接返回旧缓存）。
// 注意：不在此处直接处理队列。写入缓存后紧接着测速会被 KV 节流（1.2s/次）拖住，
// 导致 waitUntil 超时、本轮处理被中断。交给下一次 cron 触发即可。
async function handleReset(env, ctx, corsHeaders) {
  try {
    await env.LINKS_KV.put('upstream_hash', 'reset-' + Date.now());
    const output = await aggregateLinks(env);
    const pending = (output.match(/#未处理/g) || []).length;
    return new Response(`已强制重建，${pending} 个节点重新入队，将由定时任务继续处理`, {
      headers: { ...corsHeaders, 'Content-Type': 'text/plain; charset=utf-8' }
    });
  } catch (e) {
    return new Response('Reset Error: ' + e.message, {
      status: 500,
      headers: { ...corsHeaders, 'Content-Type': 'text/plain; charset=utf-8' }
    });
  }
}

// 诊断端点：直接测试指定端点的连通性，返回每次尝试的结果与耗时。
// 用法：/probe?ip=1.2.3.4:443&ip=5.6.7.8:443
async function handleProbe(url) {
  const targets = url.searchParams.getAll('ip').slice(0, 5);
  if (targets.length === 0) {
    return new Response('用法：/probe?ip=1.2.3.4:443&ip=5.6.7.8:443', {
      headers: { 'Content-Type': 'text/plain; charset=utf-8' }
    });
  }

  const lines = [];
  for (const target of targets) {
    const endpoint = parseEndpoint(target);
    if (!endpoint) {
      lines.push(`${target}  解析失败`);
      continue;
    }
    const started = Date.now();
    const result = await measureEndpoint(endpoint);
    const cost = Date.now() - started;
    lines.push(
      result.error
        ? `${target}  失败: ${result.error}  (耗时 ${cost}ms)`
        : `${target}  成功: ${result.latencyMs}ms  (总耗时 ${cost}ms)`
    );
  }

  return new Response(lines.join('\n'), {
    headers: { 'Content-Type': 'text/plain; charset=utf-8' }
  });
}

// 归属诊断端点：对指定 IP 逐个跑三个批量源，返回每源原始结果，便于定位是"源失败"还是"投票无多数"。
// 用法：/geo?ip=1.2.3.4&ip=5.6.7.8
async function handleGeoProbe(url) {
  const ips = url.searchParams.getAll('ip')
    .map(getCleanIp)
    .filter(Boolean)
    .slice(0, 20);

  if (ips.length === 0) {
    return new Response('用法：/geo?ip=1.2.3.4&ip=5.6.7.8', {
      headers: { 'Content-Type': 'text/plain; charset=utf-8' }
    });
  }

  const [primary, secondary, geojs] = await Promise.all([
    getIpLocationBatch(ips),
    getIpLocationBatchSecondary(ips),
    getIpLocationBatchGeoJs(ips)
  ]);

  const lines = [];
  for (const ip of ips) {
    const p = primary.results.get(ip) || (primary.blocked.has(ip) ? 'CLOUDFLARE' : '-');
    const s = secondary.results.get(ip) || '-';
    const g = geojs.results.get(ip) || '-';
    lines.push(`${ip}\n  ip-api=${p}  countries.dev=${s}  geojs=${g}`);
  }
  lines.push('');
  lines.push(`源状态: ip-api=${primary.status}, countries.dev=${secondary.status}, geojs=${geojs.status}`);

  return new Response(lines.join('\n'), {
    headers: { 'Content-Type': 'text/plain; charset=utf-8' }
  });
}

// ==================== /fetch 端点（直接返回当前缓存） ====================
async function handleFetch(env, corsHeaders, ctx) {
  // 始终直接返回当前缓存内容（可能包含 #未处理 与已完成结果混合的状态）
  try {
    const cached = await env.LINKS_KV.get('cached_aggregate');
    if (cached !== null && cached !== undefined) {
      // 处理进度由 cron 定时任务或 /process-now、/reprocess 推进；
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

const BATCH_MAX_IPS = 100;
// 单轮处理的端点上限：受时间预算约束，过大会导致尾部节点全部超时。
const MAX_PER_RUN = 200;
// 并发连接数：Workers 单次调用最多 6 个同时建连，用满 6 个。
const SPEED_TEST_CONCURRENCY = 6;
// 每个端点测速次数：跨境节点首次握手常失败，多次尝试可避免误删。
const SPEED_TEST_ATTEMPTS = 3;
// 单轮时间预算：必须小于 cron 周期（5 分钟），否则两轮会重叠，
// 同一节点被重复测速并互相覆盖写回。留 1 分钟余量给 KV 写入与收尾。
const CRON_RUN_BUDGET_MS = 4 * 60 * 1000;
const HTTP_RUN_BUDGET_MS = 25000;
// 单次建连超时：跨境节点建立 TLS 连接可能需 1-4 秒，2 秒会误判大量可用节点。
const CONNECT_TIMEOUT_MS = 4000;
// 延迟保留区间（含端点）：下限不宜过高，Cloudflare 边缘到目标常在 10-60ms；
// 只有极低值（<10ms）才基本可判定为劫持/本地回环。
const LATENCY_MIN_MS = 10;
const LATENCY_MAX_MS = 800;
// 单次 invocation 的外部子请求预算（Free 版上限 50，预留 6 次给 KV/杂项）
const SUBREQUEST_BUDGET = 44;
// 上游拉取并发（与 6 个并发连接上限对齐）
const AGGREGATE_CONCURRENCY = 6;
// 上游聚合与 IP 队列分属不同 invocation，各自使用独立子请求预算。
const AGGREGATE_FETCH_BUDGET = SUBREQUEST_BUDGET;
// ip-api 判定为 Cloudflare(AS13335) 的 IP 用此标记，不再当普通地区节点
const CLOUDFLARE_MARK = 'CLOUDFLARE';

// 批量 IP 归属地接口（ip-api.com 免费批量，POST，每次最多 100 个 IP）
const BATCH_API_URL = 'http://ip-api.com/batch';
const BATCH_API_URL_SECONDARY = 'https://countries.dev/ip?fields=name';
const BATCH_API_URL_GEOJS = 'https://get.geojs.io/v1/ip/country.json?ip=';
// 定时触发：区分「重新拉取上游」与「增量处理 IP」
const AGGREGATE_CRON = '0 */3 * * *'; // 每 3 小时拉取一次上游订阅
const PROCESS_CRON = '*/5 * * * *'; // 每 5 分钟处理一批（Free 计划 KV 写入额度有限）

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

async function getIpLocationBatchGeoJs(cleanIps) {
  try {
    const url = `${BATCH_API_URL_GEOJS}${encodeURIComponent(cleanIps.join(','))}`;
    const resp = await fetchWithTimeout(url, {}, 5000);
    if (!resp.ok) {
      return { results: new Map(), status: resp.status === 429 ? 'rate_limited' : 'error' };
    }

    const data = await resp.json();
    const results = new Map();
    const items = Array.isArray(data) ? data : [data];
    const requestedIps = new Set(cleanIps);
    for (const item of items) {
      const ip = item && getCleanIp(String(item.ip || ''));
      const code = String(item && item.country || '').toUpperCase();
      if (!ip || !requestedIps.has(ip) || !/^[A-Z]{2}$/.test(code)) continue;
      const name = countryCodeMap[code] || item.name || '未知';
      results.set(ip, `${code}${name}`);
    }
    return { results, status: 'ok' };
  } catch (e) {
    console.error('[IP API] GeoJS batch query failed:', e.message);
    return { results: new Map(), status: 'error' };
  }
}

// 三个免费批量接口按国家代码投票；至少两家一致才采纳。
async function resolveIpLocations(uniqueIps) {
  const resolved = new Map();
  const unresolved = new Map();
  let status = 'ok';

  for (let i = 0; i < uniqueIps.length; i += BATCH_MAX_IPS) {
    const batch = uniqueIps.slice(i, i + BATCH_MAX_IPS);
    const [primary, secondary, geojs] = await Promise.all([
      getIpLocationBatch(batch),
      getIpLocationBatchSecondary(batch),
      getIpLocationBatchGeoJs(batch)
    ]);

    for (const ip of batch) {
      if (primary.blocked.has(ip)) {
        resolved.set(ip, CLOUDFLARE_MARK);
        continue;
      }

      const primaryResult = primary.results.get(ip);
      const secondaryResult = secondary.results.get(ip);
      const geojsResult = geojs.results.get(ip);
      const sources = [
        { name: 'ip-api', result: primaryResult, status: primary.status },
        { name: 'countries.dev', result: secondaryResult, status: secondary.status },
        { name: 'GeoJS', result: geojsResult, status: geojs.status }
      ];
      const votes = new Map();
      for (const source of sources) {
        const code = getCountryResultKey(source.result);
        if (!code) continue;
        const vote = votes.get(code) || { count: 0, result: source.result };
        vote.count++;
        votes.set(code, vote);
      }
      const rankedVotes = [...votes.values()].sort((left, right) => right.count - left.count);
      if (rankedVotes.length && rankedVotes[0].count >= 2 &&
          rankedVotes[0].count > (rankedVotes[1]?.count || 0)) {
        resolved.set(ip, rankedVotes[0].result);
      } else {
        const reasons = [];
        const returned = sources
          .map(source => `${source.name}:${getCountryResultKey(source.result) || '-'}`)
          .filter(source => !source.endsWith(':-'));
        const distinctCodes = new Set(
          sources.map(source => getCountryResultKey(source.result)).filter(Boolean)
        );
        if (returned.length > 1) {
          reasons.push(`批量结果无多数(${returned.join(',')})`);
        }
        for (const source of sources) {
          if (!getCountryResultKey(source.result)) {
            reasons.push(getLookupFailureReason(source.name, source.status));
          }
        }
        unresolved.set(ip, {
          reason: reasons.join('、') || '批量接口未达成多数',
          // 三家都返回了结果但互相不同 → 数据本身矛盾，重试无意义；否则视为查询失败可重试。
          conflict: distinctCodes.size >= 2
        });
      }
    }

    if (primary.results.size === 0 && secondary.results.size === 0 && geojs.results.size === 0 &&
        primary.blocked.size === 0 &&
        primary.status !== 'ok' && secondary.status !== 'ok' && geojs.status !== 'ok') {
      status = [primary.status, secondary.status, geojs.status].includes('rate_limited')
        ? 'rate_limited'
        : 'error';
      break;
    }
  }

  return { resolved, unresolved, status };
}

function getLookupFailureReason(apiName, status) {
  if (status === 'rate_limited') return `${apiName}限流(429)`;
  if (status === 'error') return `${apiName}请求失败`;
  return `${apiName}未返回归属`;
}

function getCountryResultKey(result) {
  const match = String(result || '').trim().match(/^([A-Z]{2})/i);
  if (!match) return null;
  const code = match[1].toUpperCase();
  return code === 'UN' ? null : code;
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

    for (const line of allLines) {
      const trimmed = line.trim();
      if (!trimmed || trimmed.startsWith('#')) continue;
      totalLines++;
      if (trimmed.endsWith('#未处理')) unprocessed++;
    }

    const processed = totalLines - unprocessed;
    cacheInfo = `已缓存（约 ${totalLines} 条记录，已处理 ${processed}，未处理 ${unprocessed}）`;
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
      <button onclick="reprocess()" id="reprocessBtn" class="retry-btn">重新处理全部</button>
      <button onclick="forceReset()" id="resetBtn" class="retry-btn">强制重建</button>
      <div id="status" class="status"></div>
    </div>
    
    <div class="tips">
      <h3>使用说明</h3>
      <ul>
        <li><strong>填写订阅链接：</strong>每行一个链接，以 <code>#</code> 开头的行视为注释跳过；多个链接的相同内容会自动去重，保留首次出现顺序</li>
        <li><strong>读取方式：</strong>访问 <code>/fetch</code> 直接返回缓存，不触发后台任务；未完成项由每 5 分钟一次的定时任务继续推进（单轮最多处理 300 个节点，限时 4 分钟）</li>
        <li><strong>结果格式：</strong>每行原有 <code>#</code> 之后的文字会被丢弃，由程序重新追加结果标记</li>
        <li><strong>IP 归属地（三源投票）：</strong><code>ip-api</code>、<code>countries.dev</code>、<code>GeoJS</code> 三个免费免 Key 的批量接口并行查询，国家代码至少两家一致才采纳</li>
        <li><strong>IP 输出：</strong><code>1.2.3.4:443#JP日本|92ms</code>。延迟是 Worker 到目标的 TCP 建连时间，多次探测取最快值，不是下载速度</li>
        <li><strong>延迟过滤：</strong>只保留 <code>${LATENCY_MIN_MS}ms ~ ${LATENCY_MAX_MS}ms</code> 的节点。低于 ${LATENCY_MIN_MS}ms（多为不可用的劫持/内网响应）或高于 ${LATENCY_MAX_MS}ms（过慢）的节点会被<strong>直接删除</strong></li>
        <li><strong>优选域名：</strong>形如 <code>cf.example.com:443</code> 的行只测延迟、不查归属地（Anycast 域名的国家信息无参考价值），输出 <code>cf.example.com:443|38ms</code>，并与 IP 一起按延迟排序</li>
        <li><strong>失败即删除：</strong>归属地无法确认（三源无多数）、测速失败的节点都会被<strong>直接移除</strong>，不再保留失败标记；唯一例外是三个批量接口同时不可用时，节点保持 <code>#未处理</code> 等待下一轮，避免上游故障导致误删</li>
        <li><strong>按钮说明：</strong>「保存链接」写入订阅并刷新；「立即拉取并刷新」重新拉取上游（内容未变时不重建）；「立即处理」推进当前待处理队列；「重新处理全部」清空已完成结果并重跑当前列表；「强制重建」忽略内容哈希，重新拉取上游并把全部节点重新入队（节点被大量删除后用它恢复）</li>
        <li><strong>状态标记：</strong><code>#未处理</code> 排队中；<code>#Cloudflare</code> 为 Cloudflare 出站节点；<code>#CN中国</code> 节点会被自动移除</li>
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

    async function reprocess() {
      const btn = document.getElementById('reprocessBtn');
      const status = document.getElementById('status');

      if (!confirm('将清空所有已完成的归属地/延迟结果，重新处理全部节点。确定继续吗？')) return;

      btn.disabled = true;
      status.style.display = 'none';
      status.textContent = '正在重置并重新处理...';
      status.className = 'status';
      status.style.display = 'block';

      try {
        const res = await fetch('/reprocess', { method: 'POST' });
        const text = await res.text();
        if (res.ok) {
          status.className = 'status success';
          status.textContent = '✓ ' + text;
        } else {
          status.className = 'status error';
          status.textContent = '✗ 重新处理失败: ' + text;
        }
      } catch (e) {
        status.className = 'status error';
        status.textContent = '✗ 错误: ' + e.message;
      }

      btn.disabled = false;
    }

    async function forceReset() {
      const btn = document.getElementById('resetBtn');
      const status = document.getElementById('status');

      if (!confirm('将忽略内容哈希，重新拉取上游并把全部节点重新入队。确定继续吗？')) return;

      btn.disabled = true;
      status.style.display = 'none';
      status.textContent = '正在强制重建...';
      status.className = 'status';
      status.style.display = 'block';

      try {
        const res = await fetch('/reset', { method: 'POST' });
        const text = await res.text();
        if (res.ok) {
          status.className = 'status success';
          status.textContent = '✓ ' + text;
        } else {
          status.className = 'status error';
          status.textContent = '✗ 强制重建失败: ' + text;
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
