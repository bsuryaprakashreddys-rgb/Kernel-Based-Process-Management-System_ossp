// Bridge: runs the server.c binary, parses its stdout, and exposes it over HTTP/SSE.
// High-reliability bridge with worker race-condition protection, file explorer API, and benchmark runner.
const http = require('http');
const net = require('net');
const fs = require('fs');
const path = require('path');
const { spawn, spawnSync } = require('child_process');

const PORT = +process.env.PORT || 3000;
const ROOT = __dirname;
const BIN = path.join(ROOT, 'bin', 'server');
const SRC = path.join(ROOT, 'server.c');
const FILES_DIR = path.join(ROOT, 'files');

fs.mkdirSync(path.join(ROOT, 'bin'), { recursive: true });
fs.mkdirSync(FILES_DIR, { recursive: true });

// Auto-compile if binary missing or outdated
if (!fs.existsSync(BIN) || fs.statSync(BIN).mtimeMs < fs.statSync(SRC).mtimeMs) {
  console.log('[BRIDGE] Compiling server.c...');
  const cc = spawnSync('cc', ['-o', BIN, SRC, '-pthread'], { stdio: 'inherit' });
  if (cc.error || cc.status) {
    console.error(cc.error ? cc.error.message : `cc exited with code ${cc.status}`);
    process.exit(1);
  }
}

const S = { online: false, port: 8080, total: 0, completed: 0 };
const workers = [];
const logs = [];
const active = new Map();
const recentDone = new Map();
const clients = new Set();
let seq = 0;

const emit = (event, data) => {
  const payload = `event: ${event}\ndata: ${JSON.stringify(data)}\n\n`;
  for (const client of clients) {
    try {
      client.write(payload);
    } catch (_) {}
  }
};

function getWorker(pid) {
  if (active.has(pid)) return active.get(pid);
  if (recentDone.has(pid)) return recentDone.get(pid);
  return null;
}

function getOrCreateWorker(pid) {
  let w = getWorker(pid);
  if (!w) {
    w = {
      id: ++seq,
      pid,
      op: null,
      file: null,
      created: Date.now(),
      status: 'CREATED',
      latency: null,
      total: null,
      throughput: null,
      done: null
    };
    active.set(pid, w);
    workers.push(w);
    S.total++;
    if (workers.length > 1000) workers.shift();
  }
  return w;
}

const pushW = (w) => emit('worker', { w, total: S.total, completed: S.completed });

function parse(line, ts) {
  let m;
  if ((m = line.match(/Listening on port (\d+)/))) {
    S.online = true;
    S.port = +m[1];
    emit('status', S);
  } else if ((m = line.match(/^\[SERVER\] Worker (\d+) created/))) {
    const pid = +m[1];
    const w = getOrCreateWorker(pid);
    pushW(w);
  } else if ((m = line.match(/^\[WORKER (\d+)\] Processing (.*) request/))) {
    const pid = +m[1];
    const w = getOrCreateWorker(pid);
    w.op = m[2];
    if (w.status !== 'DONE') {
      w.status = 'PROCESSING';
    }
    pushW(w);
  } else if ((m = line.match(/^\[COMPLETION QUEUE\] worker=(\d+) op=(\S*) file=(\S*) latency=([\d.]+)ms total=(\d+) throughput=([\d.]+)/))) {
    const pid = +m[1];
    const w = getOrCreateWorker(pid);
    const latency = parseFloat(m[4]) || 0;
    const total = parseInt(m[5], 10) || 0;
    const throughput = parseFloat(m[6]) || 0;

    Object.assign(w, {
      op: m[2],
      file: m[3],
      latency,
      total,
      throughput,
      status: 'DONE',
      done: ts
    });

    if (active.has(pid)) {
      active.delete(pid);
      recentDone.set(pid, w);
      setTimeout(() => recentDone.delete(pid), 30000);
      S.completed++;
    } else if (!recentDone.has(pid)) {
      recentDone.set(pid, w);
      setTimeout(() => recentDone.delete(pid), 30000);
      S.completed++;
    }

    pushW(w);
  }
}

function log(msg, ts = Date.now()) {
  const e = { id: ++seq, ts, msg };
  logs.push(e);
  if (logs.length > 2000) logs.shift();
  emit('log', e);
}

function feed(stream) {
  let buf = '';
  stream.on('data', (chunk) => {
    buf += chunk;
    let idx;
    while ((idx = buf.indexOf('\n')) >= 0) {
      const line = buf.slice(0, idx).trim();
      buf = buf.slice(idx + 1);
      if (line && !/^[-=]+$/.test(line) && !/^ASYNCHRONOUS/.test(line)) {
        const ts = Date.now();
        log(line, ts);
        parse(line, ts);
      }
    }
  });
}

let child = null;
function startServerProcess() {
  if (child) {
    try { child.kill(); } catch (_) {}
  }
  child = spawn(BIN, [], { cwd: FILES_DIR });
  log(`[SERVER] Spawned bin/server (pid ${child.pid})`);

  child.on('error', (err) => {
    S.online = false;
    log(`[SERVER] Spawn error: ${err.message}`);
    emit('status', S);
  });

  feed(child.stdout);
  feed(child.stderr);

  child.on('exit', (code, sig) => {
    S.online = false;
    log(`[SERVER] Process exited (${code ?? sig})`);
    emit('status', S);
  });
}

startServerProcess();

for (const sig of ['SIGINT', 'SIGTERM']) {
  process.on(sig, () => {
    if (child) child.kill();
    process.exit();
  });
}

// Wire format matching client.c:
// struct Request { char operation[10]; char filename[256]; char data[1024]; } -> 1290 bytes
const sendRequest = (op, filename, data = '') =>
  new Promise((resolve, reject) => {
    const buf = Buffer.alloc(1290);
    buf.write(op, 0);
    buf.write(filename, 10);
    buf.write(data, 266);

    const socket = net.connect(S.port, '127.0.0.1');
    let out = '';
    const timer = setTimeout(() => {
      socket.destroy();
      reject(new Error('Connection timed out after 5000ms'));
    }, 5000);

    socket.on('connect', () => socket.write(buf));
    socket.on('data', (chunk) => { out += chunk; });
    socket.on('error', (err) => {
      clearTimeout(timer);
      reject(err);
    });
    socket.on('close', () => {
      clearTimeout(timer);
      out ? resolve(out) : reject(new Error('No response from server'));
    });
  });

function listFiles() {
  try {
    const dirents = fs.readdirSync(FILES_DIR, { withFileTypes: true });
    return dirents
      .filter((d) => d.isFile() && !d.name.startsWith('.'))
      .map((d) => {
        const full = path.join(FILES_DIR, d.name);
        const st = fs.statSync(full);
        return {
          name: d.name,
          size: st.size,
          mtime: st.mtimeMs
        };
      })
      .sort((a, b) => b.mtime - a.mtime);
  } catch (err) {
    return [];
  }
}

const server = http.createServer((req, res) => {
  const urlObj = new URL(req.url, `http://127.0.0.1:${PORT}`);
  const pathname = urlObj.pathname;

  // Static HTML
  if (pathname === '/') {
    res.writeHead(200, {
      'content-type': 'text/html; charset=utf-8',
      'cache-control': 'no-cache'
    });
    return fs.createReadStream(path.join(ROOT, 'public', 'index.html')).pipe(res);
  }

  // SSE Stream
  if (pathname === '/api/stream') {
    res.writeHead(200, {
      'content-type': 'text/event-stream',
      'cache-control': 'no-cache',
      connection: 'keep-alive'
    });
    res.write(`event: snapshot\ndata: ${JSON.stringify({ status: S, workers, logs, files: listFiles() })}\n\n`);
    clients.add(res);
    req.on('close', () => clients.delete(res));
    return;
  }

  // Send Single Request
  if (pathname === '/api/request' && req.method === 'POST') {
    let body = '';
    req.on('data', (d) => {
      body += d;
      if (body.length > 16384) req.destroy();
    });
    req.on('end', async () => {
      const sendJson = (code, obj) => {
        res.writeHead(code, { 'content-type': 'application/json' });
        res.end(JSON.stringify(obj));
      };

      try {
        const { operation, filename, data = '' } = JSON.parse(body);
        if (!['READ', 'WRITE'].includes(operation)) {
          return sendJson(400, { error: 'Operation must be READ or WRITE' });
        }
        if (!/^(?!\.)[\w.\-]{1,255}$/.test(filename || '')) {
          return sendJson(400, { error: 'Invalid filename. Use alphanumeric, dash, dot, or underscore.' });
        }
        if (Buffer.byteLength(data) > 1023 || data.includes('\0')) {
          return sendJson(400, { error: 'Data payload exceeds 1023 bytes or contains null bytes' });
        }
        if (!S.online) {
          return sendJson(503, { error: 'Server process is currently offline' });
        }

        const response = await sendRequest(operation, filename, data);
        sendJson(200, { ok: true, response, files: listFiles() });
      } catch (err) {
        sendJson(502, { ok: false, error: err.message });
      }
    });
    return;
  }

  // Files List
  if (pathname === '/api/files' && req.method === 'GET') {
    res.writeHead(200, { 'content-type': 'application/json' });
    res.end(JSON.stringify({ files: listFiles() }));
    return;
  }

  // Read File Content
  if (pathname === '/api/file' && req.method === 'GET') {
    const filename = path.basename(urlObj.searchParams.get('name') || '');
    if (!filename || filename.startsWith('.')) {
      res.writeHead(400, { 'content-type': 'application/json' });
      return res.end(JSON.stringify({ error: 'Invalid filename' }));
    }
    const fullPath = path.join(FILES_DIR, filename);
    if (!fs.existsSync(fullPath)) {
      res.writeHead(404, { 'content-type': 'application/json' });
      return res.end(JSON.stringify({ error: 'File not found' }));
    }
    try {
      const content = fs.readFileSync(fullPath, 'utf8');
      res.writeHead(200, { 'content-type': 'application/json' });
      res.end(JSON.stringify({ filename, content }));
    } catch (err) {
      res.writeHead(500, { 'content-type': 'application/json' });
      res.end(JSON.stringify({ error: err.message }));
    }
    return;
  }

  // Delete File
  if (pathname === '/api/file' && req.method === 'DELETE') {
    const filename = path.basename(urlObj.searchParams.get('name') || '');
    if (!filename || filename.startsWith('.')) {
      res.writeHead(400, { 'content-type': 'application/json' });
      return res.end(JSON.stringify({ error: 'Invalid filename' }));
    }
    const fullPath = path.join(FILES_DIR, filename);
    try {
      if (fs.existsSync(fullPath)) {
        fs.unlinkSync(fullPath);
      }
      res.writeHead(200, { 'content-type': 'application/json' });
      res.end(JSON.stringify({ ok: true, filename, files: listFiles() }));
    } catch (err) {
      res.writeHead(500, { 'content-type': 'application/json' });
      res.end(JSON.stringify({ error: err.message }));
    }
    return;
  }

  // Concurrency Benchmark / Stress Test
  if (pathname === '/api/benchmark' && req.method === 'POST') {
    let body = '';
    req.on('data', (d) => { body += d; });
    req.on('end', async () => {
      const sendJson = (code, obj) => {
        res.writeHead(code, { 'content-type': 'application/json' });
        res.end(JSON.stringify(obj));
      };
      if (!S.online) {
        return sendJson(503, { error: 'Server offline' });
      }
      try {
        const { count = 10, concurrency = 5 } = JSON.parse(body || '{}');
        const totalReqs = Math.min(Math.max(1, count), 50);
        const limit = Math.min(Math.max(1, concurrency), 10);

        const tasks = Array.from({ length: totalReqs }, (_, i) => ({
          id: i + 1,
          op: i % 2 === 0 ? 'WRITE' : 'READ',
          file: `bench_${(i % 5) + 1}.txt`,
          data: `Benchmark sample payload #${i + 1} at ${new Date().toISOString()}`
        }));

        const results = [];
        let index = 0;
        async function runWorker() {
          while (index < tasks.length) {
            const current = tasks[index++];
            const t0 = Date.now();
            try {
              const reply = await sendRequest(current.op, current.file, current.data);
              results.push({ id: current.id, op: current.op, file: current.file, latency: Date.now() - t0, ok: true, reply });
            } catch (err) {
              results.push({ id: current.id, op: current.op, file: current.file, latency: Date.now() - t0, ok: false, error: err.message });
            }
          }
        }

        const workersList = [];
        for (let i = 0; i < limit; i++) {
          workersList.push(runWorker());
        }
        await Promise.all(workersList);

        sendJson(200, {
          total: totalReqs,
          completed: results.filter((r) => r.ok).length,
          failed: results.filter((r) => !r.ok).length,
          results
        });
      } catch (err) {
        sendJson(500, { error: err.message });
      }
    });
    return;
  }

  // 404
  res.writeHead(404, { 'content-type': 'application/json' });
  res.end(JSON.stringify({ error: 'Endpoint not found' }));
});

server.listen(PORT, '127.0.0.1', () => {
  console.log(`[BRIDGE] Dashboard running at: http://localhost:${PORT}`);
});

// SSE keep-alive heartbeats every 15s
setInterval(() => {
  for (const c of clients) {
    try {
      c.write(':\n\n');
    } catch (_) {}
  }
}, 15000);