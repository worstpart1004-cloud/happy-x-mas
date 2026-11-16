// Merry Christmas 마을 서버 — 외부 패키지 없이 Node만으로 동작합니다.
// - GET  /            : 게임 (index.html)
// - GET  /events      : 실시간 소식 (Server-Sent Events) — 접속자 위치, 집 꾸미기, 눈사람, 소원
// - POST /presence    : 내 위치/모습 보내기
// - POST /db/set      : 집 · 눈사람 · 소원 저장
// - POST /db/delete   : 저장된 항목 지우기
// 저장 데이터는 data.json 에 보관합니다 (무료 서버는 다시 시작하면 지워질 수 있어요).
'use strict';
const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');

const PORT = process.env.PORT || 3000;
const ROOT = __dirname;
const DATA_FILE = path.join(ROOT, 'data.json');
const MAX_BODY = 64 * 1024;          // 요청 하나의 최대 크기
const MAX_PEERS = 60;                // 동시에 들어올 수 있는 사람 수
const MAX_DOCS = 400;                // 저장 항목 최대 개수
// 게임이 쓰는 저장 위치만 허용
const PATH_OK = /^(houses\/h[1-6]|village\/(snow|wishes)|live\/t[a-z0-9]{4,14})$/;

// ---------- 저장소 ----------
let DB = {};
try { DB = JSON.parse(fs.readFileSync(DATA_FILE, 'utf8')) || {}; } catch (e) { DB = {}; }
let saveTimer = null;
function persist() {
  clearTimeout(saveTimer);
  saveTimer = setTimeout(() => {
    const keep = {}; for (const k in DB) if (!k.startsWith('live/')) keep[k] = DB[k]; // 접속 표시는 저장하지 않음
    fs.writeFile(DATA_FILE + '.tmp', JSON.stringify(keep), err => { if (!err) fs.rename(DATA_FILE + '.tmp', DATA_FILE, () => { }); });
  }, 1000);
}

// ---------- 접속자 ----------
const clients = new Map(); // peer -> { res, presence, joined, last }
function send(res, event, data) { try { res.write(`event: ${event}\ndata: ${JSON.stringify(data)}\n\n`); } catch (e) { } }
function broadcast(event, data, except) { for (const [id, c] of clients) if (id !== except) send(c.res, event, data); }
function peerInfo(id, c) { return { peer: id, presence: c.presence }; }

// ---------- 도우미 ----------
function readBody(req) {
  return new Promise((resolve, reject) => {
    let size = 0; const chunks = [];
    req.on('data', d => { size += d.length; if (size > MAX_BODY) { reject(new Error('too_big')); req.destroy(); } else chunks.push(d); });
    req.on('end', () => { try { resolve(JSON.parse(Buffer.concat(chunks).toString('utf8') || '{}')); } catch (e) { reject(new Error('bad_json')); } });
    req.on('error', reject);
  });
}
function json(res, code, obj) { res.writeHead(code, { 'Content-Type': 'application/json; charset=utf-8', 'Cache-Control': 'no-store' }); res.end(JSON.stringify(obj)); }
const rate = new Map(); // 너무 많은 요청 막기 (peer 별 초당 40회)
function limited(id) { const now = Date.now(); const r = rate.get(id) || { t: now, n: 0 }; if (now - r.t > 1000) { r.t = now; r.n = 0; } r.n++; rate.set(id, r); return r.n > 40; }
const validPeer = id => typeof id === 'string' && /^p[a-z0-9]{6,24}$/.test(id);

// ---------- 서버 ----------
let indexHtml = null;
function loadIndex() { try { indexHtml = fs.readFileSync(path.join(ROOT, 'index.html')); } catch (e) { indexHtml = Buffer.from('<h1>index.html 이 없어요</h1>'); } }
loadIndex();

const server = http.createServer(async (req, res) => {
  const url = new URL(req.url, 'http://x');
  try {
    if (req.method === 'GET' && (url.pathname === '/' || url.pathname === '/index.html')) {
      res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-cache' }); return res.end(indexHtml);
    }
    if (req.method === 'GET' && url.pathname === '/health') return json(res, 200, { ok: true, people: clients.size });

    if (req.method === 'GET' && url.pathname === '/events') {
      const id = url.searchParams.get('peer');
      if (!validPeer(id)) return json(res, 400, { error: 'bad_peer' });
      if (!clients.has(id) && clients.size >= MAX_PEERS) return json(res, 503, { error: 'full' });
      res.writeHead(200, { 'Content-Type': 'text/event-stream; charset=utf-8', 'Cache-Control': 'no-cache, no-transform', 'Connection': 'keep-alive', 'X-Accel-Buffering': 'no' });
      res.write('retry: 3000\n\n');
      const old = clients.get(id); if (old) { try { old.res.end(); } catch (e) { } }
      const c = { res, presence: old ? old.presence : null, joined: old ? old.joined : false };
      clients.set(id, c);
      const peers = []; for (const [pid, pc] of clients) if (pid !== id && pc.joined) peers.push(peerInfo(pid, pc));
      send(res, 'init', { db: DB, peers });
      const ping = setInterval(() => { try { res.write(': ping\n\n'); } catch (e) { } }, 15000);
      req.on('close', () => {
        clearInterval(ping);
        if (clients.get(id) !== c) return; // 새 연결로 바뀐 경우
        clients.delete(id); rate.delete(id);
        if (c.joined) broadcast('peer', { type: 'left', peer: id });
      });
      return;
    }

    if (req.method === 'POST' && url.pathname === '/presence') {
      const b = await readBody(req); const id = b.peer; const c = clients.get(id);
      if (!validPeer(id) || !c) return json(res, 409, { error: 'not_connected' });
      if (limited(id)) return json(res, 429, { error: 'slow_down' });
      if (!b.presence || typeof b.presence !== 'object') return json(res, 400, { error: 'invalid_argument' });
      c.presence = b.presence; const type = c.joined ? 'updated' : 'joined'; c.joined = true;
      broadcast('peer', { type, peer: id, presence: c.presence }, id);
      return json(res, 200, { ok: true });
    }

    if (req.method === 'POST' && (url.pathname === '/db/set' || url.pathname === '/db/delete')) {
      const b = await readBody(req); const id = b.peer;
      if (!validPeer(id) || !clients.has(id)) return json(res, 409, { error: 'not_connected' });
      if (limited(id)) return json(res, 429, { error: 'slow_down' });
      const p = b.path;
      if (typeof p !== 'string' || !PATH_OK.test(p)) return json(res, 400, { error: 'invalid_argument' });
      if (url.pathname === '/db/set') {
        if (!b.data || typeof b.data !== 'object') return json(res, 400, { error: 'invalid_argument' });
        if (!(p in DB) && Object.keys(DB).length >= MAX_DOCS) return json(res, 507, { error: 'full' });
        DB[p] = b.data;
      } else delete DB[p];
      broadcast('doc', { path: p, data: DB[p] === undefined ? null : DB[p] });
      if (!p.startsWith('live/')) persist();
      return json(res, 200, { ok: true });
    }

    res.writeHead(404, { 'Content-Type': 'text/plain; charset=utf-8' }); res.end('없는 주소예요');
  } catch (e) {
    if (!res.headersSent) json(res, e.message === 'too_big' ? 413 : 400, { error: e.message || 'error' });
  }
});

// 오래된 접속 표시(live/…) 정리
setInterval(() => {
  const now = Date.now();
  for (const k of Object.keys(DB)) if (k.startsWith('live/') && now - (+(DB[k] && DB[k].ts) || 0) > 60000) { delete DB[k]; broadcast('doc', { path: k, data: null }); }
}, 30000);

server.listen(PORT, () => console.log('🎄 Merry Christmas 마을 서버가 열렸어요 — 포트 ' + PORT));
