/* =====================================================================
   STINGER – Interceptor Challenge · Online sunucu (Cloudflare Workers)
   Ücretsiz plan, kart gerekmez. Tek bir Durable Object ("Lobby") tüm
   oyuncuları, odaları ve liderlik kayıtlarını (SQLite) yönetir.
   ===================================================================== */
import { DurableObject } from 'cloudflare:workers';

const VERSION = '1.2.0';
const CODE_CHARS = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
const DIFFS = ['NORMAL', 'HARD', 'EXPERT'];

/* ---------------- Worker: yönlendirme ---------------- */
export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const lobby = env.LOBBY.get(env.LOBBY.idFromName('global'));
    if ((request.headers.get('Upgrade') || '').toLowerCase() === 'websocket') return lobby.fetch(request);
    if (url.pathname === '/health') return lobby.fetch(new Request('https://lobby/health'));
    return env.ASSETS.fetch(request);   // oyunun web sürümü (public/index.html)
  }
};

/* ---------------- yardımcılar ---------------- */
const randInt = n => { const a = new Uint32Array(1); crypto.getRandomValues(a); return a[0] % n; };
const uid = (n = 8) => { let s = ''; const c = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789'; for (let i = 0; i < n; i++) s += c[randInt(c.length)]; return s; };
async function stableId(dev) {
  const buf = await crypto.subtle.digest('SHA-256', new TextEncoder().encode('stinger:' + dev));
  return btoa(String.fromCharCode(...new Uint8Array(buf))).replace(/\+/g, '-').replace(/\//g, '_').slice(0, 12);
}
function cleanName(s) { s = String(s || '').replace(/[\u0000-\u001f<>]/g, '').replace(/\s+/g, ' ').trim().slice(0, 16); return s.length >= 2 ? s : null; }
function cleanText(s, n) { return String(s || '').replace(/[\u0000-\u001f]/g, ' ').trim().slice(0, n); }
const clampInt = (v, a, b) => Math.max(a, Math.min(b, Math.round(+v || 0)));
// Türkiye saati (UTC+3) ile dönem anahtarı ve başlangıcı
const TR = 3 * 3600e3;
function trParts(ts) { const d = new Date(ts + TR); return { y: d.getUTCFullYear(), m: d.getUTCMonth(), d: d.getUTCDate(), wd: (d.getUTCDay() + 6) % 7 }; }
function periodStart(ts, period) {
  const p = trParts(ts);
  if (period === 'month') return Date.UTC(p.y, p.m, 1) - TR;
  if (period === 'week') return Date.UTC(p.y, p.m, p.d - p.wd) - TR;
  return Date.UTC(p.y, p.m, p.d) - TR;
}
function periodKey(ts, period) {
  const p = trParts(ts);
  if (period === 'day') return `${p.y}-${p.m + 1}-${p.d}`;
  if (period === 'month') return `${p.y}-${p.m + 1}`;
  const t = new Date(Date.UTC(p.y, p.m, p.d)); t.setUTCDate(t.getUTCDate() - p.wd + 3);
  const fy = t.getUTCFullYear(), jan4 = new Date(Date.UTC(fy, 0, 4));
  return `${fy}-W${1 + Math.round(((t - jan4) / 864e5 - 3 + ((jan4.getUTCDay() + 6) % 7)) / 7)}`;
}

/* ---------------- Durable Object: lobi ---------------- */
export class Lobby extends DurableObject {
  constructor(ctx, env) {
    super(ctx, env);
    this.env = env;
    const num = (k, d) => (env && env[k] !== undefined && env[k] !== '' ? +env[k] : d);
    this.CFG = { shotsPerPlayer: 3, introMs: num('INTRO_MS', 4500), resultMs: num('RESULT_MS', 4500), liveMs: num('LIVE_MS', 85000), quickWaitMs: num('QUICK_WAIT_MS', 8000),
      quickMax: 4, roomMaxLimit: 8, offlineGraceMs: 60000, maxShotScore: 210, maxSoloScore: 1500, keepDays: 40 };
    this.sql = ctx.storage.sql;
    this.sql.exec('CREATE TABLE IF NOT EXISTS scores (pid TEXT, name TEXT, kind TEXT, score INTEGER, hits INTEGER, shots INTEGER, diff TEXT, ts INTEGER)');
    this.sql.exec('CREATE INDEX IF NOT EXISTS scores_kind_ts ON scores(kind, ts)');
    this.players = new Map(); this.byDevice = new Map(); this.rooms = new Map();
    this.quickQueue = []; this.quickTimer = null; this.sweeper = null;
  }

  /* ---------- bağlantılar ---------- */
  async fetch(request) {
    if (new URL(request.url).pathname === '/health')
      return new Response(JSON.stringify({ ok: true, name: 'stinger-server', ver: VERSION, platform: 'cloudflare', online: this.onlineCount(), rooms: this.rooms.size }), { headers: { 'content-type': 'application/json', 'access-control-allow-origin': '*' } });
    const pair = new WebSocketPair(), [client, server] = Object.values(pair);
    server.accept(); this.attach(server);
    if (!this.sweeper) this.sweeper = setInterval(() => this.sweep(), 15000);
    return new Response(null, { status: 101, webSocket: client });
  }
  attach(ws) {
    let p = null, msgCount = 0, windowStart = Date.now(), helloBusy = false; ws.lastSeen = Date.now();
    ws.addEventListener('message', async ev => {
      ws.lastSeen = Date.now();
      const now = Date.now(); if (now - windowStart > 1000) { windowStart = now; msgCount = 0; } if (++msgCount > 60) return;
      let m; try { m = JSON.parse(typeof ev.data === 'string' ? ev.data : new TextDecoder().decode(ev.data)); } catch (e) { return; }
      if (!m || typeof m.t !== 'string') return;
      if (!p) {
        if (m.t !== 'hello' || helloBusy) return; helloBusy = true;
        const dev = cleanText(m.deviceId, 64) || uid(16);
        let pid = this.byDevice.get(dev); p = pid && this.players.get(pid);
        if (p && p.ws && p.ws !== ws) { try { p.ws.close(4000, 'replaced'); } catch (e) {} }
        if (!p) { pid = await stableId(dev); p = { pid, deviceId: dev, name: 'Atıcı', ws: null, room: null, lastChat: 0, submitted: new Set(), offlineAt: 0 }; this.players.set(pid, p); this.byDevice.set(dev, pid); }
        p.ws = ws; p.offlineAt = 0; p.name = cleanName(m.name) || p.name;
        this.send(p, 'welcome', { id: p.pid, name: p.name, online: this.onlineCount(), ver: VERSION });
        const r = p.room && this.rooms.get(p.room);
        if (r) { if (r.members.includes(p.pid)) { this.send(p, 'room', { room: this.roomPublic(r, p.pid), chat: r.chat.slice(-30) }); this.roomSys(r, 'back', { name: p.name }); } else if (r.pending.includes(p.pid)) this.send(p, 'pending', { name: r.name, code: r.code }); }
        return;
      }
      try { this.handle(p, m); } catch (e) { console.error('işleme hatası', e && e.stack || e); }
    });
    const closed = () => {
      if (!p || p.ws !== ws) return;
      p.ws = null; p.offlineAt = Date.now();
      this.quickQueue = this.quickQueue.filter(id => id !== p.pid);
      const r = p.room && this.rooms.get(p.room);
      if (r) { if (r.phase === 'playing' && r.turn && r.turn.shooter === p.pid && r.turn.phase === 'live') this.finishTurn(r, { score: 0, kind: 'offline' }); this.broadcastRoom(r); }
    };
    ws.addEventListener('close', closed); ws.addEventListener('error', closed);
  }
  sweep() {
    const now = Date.now();
    for (const p of this.players.values()) {
      if (p.ws && now - (p.ws.lastSeen || 0) > 75000) { try { p.ws.close(4001, 'timeout'); } catch (e) {} }
      if (p.ws || !p.offlineAt || now - p.offlineAt < this.CFG.offlineGraceMs) continue;
      if (p.room) { const r = this.rooms.get(p.room); if (r && r.phase !== 'playing') this.removeFromRoom(p, 'left'); else if (!r) p.room = null; }
      if (!p.room && now - p.offlineAt > 6 * 3600e3) { this.players.delete(p.pid); this.byDevice.delete(p.deviceId); }
    }
    if (Math.random() < 0.02) this.sql.exec('DELETE FROM scores WHERE ts < ?', now - this.CFG.keepDays * 864e5);
    if (!this.onlineCount() && !this.rooms.size) { clearInterval(this.sweeper); this.sweeper = null; }
  }
  send(p, t, d = {}) { if (p && p.ws && p.ws.readyState === 1) { try { p.ws.send(JSON.stringify({ t, ...d })); } catch (e) {} } }
  sendRaw(p, s) { if (p && p.ws && p.ws.readyState === 1) { try { p.ws.send(s); } catch (e) {} } }
  onlineCount() { let n = 0; for (const p of this.players.values()) if (p.ws) n++; return n; }

  /* ---------- liderlik (SQLite) ---------- */
  addScore(p, kind, score, hits, shots, diff) { this.sql.exec('INSERT INTO scores (pid,name,kind,score,hits,shots,diff,ts) VALUES (?,?,?,?,?,?,?,?)', p.pid, p.name, kind, score, hits, shots, diff, Date.now()); }
  leaderboard(period, kind, pid) {
    const now = Date.now(), from = periodStart(now, period), best = new Map();
    for (const s of this.sql.exec('SELECT pid,name,score,hits,shots,diff,ts FROM scores WHERE kind = ? AND ts >= ?', kind, from)) {
      const b = best.get(s.pid); if (!b || s.score > b.score || (s.score === b.score && s.ts < b.ts)) best.set(s.pid, s);
    }
    const rows = [...best.values()].sort((a, b) => b.score - a.score || a.ts - b.ts);
    const mi = rows.findIndex(s => s.pid === pid);
    return { period, kind, key: periodKey(now, period), total: rows.length, me: mi >= 0 ? { rank: mi + 1, score: rows[mi].score } : null,
      rows: rows.slice(0, 50).map((s, i) => ({ rank: i + 1, name: s.name, score: s.score, hits: s.hits, shots: s.shots, diff: s.diff, me: s.pid === pid })) };
  }

  /* ---------- odalar ---------- */
  roomCode() { let c; do { c = ''; for (let i = 0; i < 5; i++) c += CODE_CHARS[randInt(CODE_CHARS.length)]; } while ([...this.rooms.values()].some(r => r.code === c)); return c; }
  newRoom({ name, hostId, max, diff, quick }) {
    const r = { id: uid(8), code: this.roomCode(), name, hostId, max, diff, quick: !!quick, phase: 'lobby', members: [], pending: [], order: [], results: {}, idx: 0, turn: null, timer: null, chat: [], game: 0 };
    this.rooms.set(r.id, r); return r;
  }
  roomPublic(r, forPid) {
    const ids = [...new Set([...r.members, ...(r.order.length ? r.order : [])])];
    const mem = ids.map(id => { const p = this.players.get(id), res = r.results[id] || [];
      return { id, name: p ? p.name : '?', online: !!(p && p.ws), inRoom: r.members.includes(id), host: id === r.hostId, shots: res, total: res.reduce((a, s) => a + s.score, 0) }; });
    return { id: r.id, code: r.code, name: r.name, hostId: r.hostId, max: r.max, diff: r.diff, quick: r.quick, phase: r.phase, game: r.game, members: mem, order: r.order,
      turn: r.turn && { ...r.turn, now: Date.now() }, pending: forPid === r.hostId ? r.pending.map(id => ({ id, name: (this.players.get(id) || {}).name || '?' })) : [], shotsPerPlayer: this.CFG.shotsPerPlayer };
  }
  broadcastRoom(r) { for (const id of r.members) this.send(this.players.get(id), 'room', { room: this.roomPublic(r, id) }); }
  roomSys(r, key, args = {}) { const m = { sys: key, args, ts: Date.now() }; r.chat.push(m); if (r.chat.length > 60) r.chat.shift(); for (const id of r.members) this.send(this.players.get(id), 'chat', m); }
  openRooms() { return [...this.rooms.values()].filter(r => !r.quick && r.phase === 'lobby').map(r => ({ id: r.id, code: r.code, name: r.name, host: (this.players.get(r.hostId) || {}).name || '?', count: r.members.length, max: r.max, diff: r.diff })); }
  removeFromRoom(p, why) {
    const r = this.rooms.get(p.room); p.room = null; if (!r) return;
    r.pending = r.pending.filter(id => id !== p.pid);
    if (!r.members.includes(p.pid)) return;
    r.members = r.members.filter(id => id !== p.pid);
    this.roomSys(r, why === 'kick' ? 'kicked' : 'left', { name: p.name });
    if (r.hostId === p.pid) { r.hostId = r.quick ? null : (r.members[0] || null); if (r.hostId) this.roomSys(r, 'newhost', { name: this.players.get(r.hostId).name }); }
    if (!r.members.length) { clearTimeout(r.timer); this.rooms.delete(r.id); return; }
    if (r.phase === 'playing' && r.turn && r.turn.shooter === p.pid && r.turn.phase !== 'result') this.finishTurn(r, { score: 0, kind: 'left' });
    this.broadcastRoom(r);
  }
  startGame(r) {
    clearTimeout(r.timer);
    r.phase = 'playing'; r.game++; r.order = r.members.slice(); r.results = {}; for (const id of r.order) r.results[id] = []; r.idx = 0;
    r.pending.forEach(id => { const q = this.players.get(id); if (q) { q.room = null; this.send(q, 'rejected', { name: r.name, why: 'started' }); } }); r.pending = [];
    this.roomSys(r, 'started', { n: r.order.length });
    this.nextTurn(r);
  }
  nextTurn(r) {
    clearTimeout(r.timer);
    const n = r.order.length, total = n * this.CFG.shotsPerPlayer;
    while (r.idx < total) { const s = r.order[r.idx % n]; if (r.members.includes(s)) break; r.results[s].push({ score: 0, kind: 'left', hit: false }); r.idx++; }
    if (r.idx >= total || !r.members.length) return this.endGame(r);
    const shooter = r.order[r.idx % n], round = Math.floor(r.idx / n) + 1, p = this.players.get(shooter);
    const upcoming = []; for (let k = 1; k <= 3 && r.idx + k < total; k++) upcoming.push((this.players.get(r.order[(r.idx + k) % n]) || {}).name || '?');
    r.turn = { phase: 'intro', shooter, name: p ? p.name : '?', round, shotNo: r.results[shooter].length + 1, of: this.CFG.shotsPerPlayer, idx: r.idx, total,
      seed: 1 + randInt(2 ** 31 - 2), diff: r.diff, startedAt: Date.now(), until: Date.now() + this.CFG.introMs, upcoming };
    this.broadcastRoom(r);
    r.timer = setTimeout(() => {
      if (!p || !p.ws) { this.finishTurn(r, { score: 0, kind: 'offline' }); return; }
      r.turn.phase = 'live'; r.turn.until = Date.now() + this.CFG.liveMs; this.broadcastRoom(r);
      r.timer = setTimeout(() => this.finishTurn(r, { score: 0, kind: 'timeout' }), this.CFG.liveMs);
    }, this.CFG.introMs);
  }
  finishTurn(r, res) {
    if (!r.turn || r.turn.phase === 'result') return;
    clearTimeout(r.timer);
    const shot = { score: clampInt(res.score, 0, this.CFG.maxShotScore), hit: !!res.hit, miss: res.miss == null ? null : clampInt(res.miss, 0, 9999), kind: cleanText(res.kind || 'shot', 12), range: res.range == null ? null : clampInt(res.range, 0, 20000) };
    r.results[r.turn.shooter].push(shot);
    r.turn.phase = 'result'; r.turn.result = shot; r.turn.until = Date.now() + this.CFG.resultMs; r.idx++;
    this.broadcastRoom(r);
    r.timer = setTimeout(() => this.nextTurn(r), this.CFG.resultMs);
  }
  endGame(r) {
    clearTimeout(r.timer); r.phase = 'finished'; r.turn = null;
    const ranking = r.order.map(id => { const res = r.results[id] || [], p = this.players.get(id);
      return { id, name: p ? p.name : '?', total: res.reduce((a, s) => a + s.score, 0), hits: res.filter(s => s.hit).length, best: Math.max(0, ...res.map(s => s.score)), left: !r.members.includes(id) }; })
      .sort((a, b) => b.total - a.total || b.hits - a.hits || b.best - a.best);
    ranking.forEach((x, i) => { x.place = i + 1; });
    if (r.order.length >= 2) for (const x of ranking) { const p = this.players.get(x.id); if (p && !x.left) this.addScore(p, 'tour', x.total, x.hits, this.CFG.shotsPerPlayer, r.diff); }
    this.broadcastRoom(r);
    for (const id of r.members) this.send(this.players.get(id), 'over', { ranking, room: r.id });
    if (r.quick) r.timer = setTimeout(() => { for (const id of r.members) { const p = this.players.get(id); if (p) { p.room = null; this.send(p, 'left', { why: 'closed' }); } } this.rooms.delete(r.id); }, 60000);
  }
  quickTick() {
    this.quickQueue = this.quickQueue.filter(id => { const p = this.players.get(id); return p && p.ws && !p.room; });
    for (const id of this.quickQueue) this.send(this.players.get(id), 'quick', { state: 'waiting', count: this.quickQueue.length });
    if (this.quickQueue.length >= this.CFG.quickMax) return this.makeQuick();
    if (this.quickQueue.length >= 2 && !this.quickTimer) this.quickTimer = setTimeout(() => { this.quickTimer = null; if (this.quickQueue.length >= 2) this.makeQuick(); }, this.CFG.quickWaitMs);
  }
  makeQuick() {
    clearTimeout(this.quickTimer); this.quickTimer = null;
    const ids = this.quickQueue.splice(0, this.CFG.quickMax);
    const r = this.newRoom({ name: 'Hızlı Maç', hostId: null, max: ids.length, diff: 'NORMAL', quick: true });
    for (const id of ids) { const p = this.players.get(id); r.members.push(id); p.room = r.id; this.send(p, 'quick', { state: 'matched' }); }
    this.broadcastRoom(r); this.roomSys(r, 'quickstart', { n: ids.length });
    r.timer = setTimeout(() => this.startGame(r), 3000);
    if (this.quickQueue.length) this.quickTick();
  }

  /* ---------- mesajlar ---------- */
  handle(p, m) {
    const r = p.room ? this.rooms.get(p.room) : null;
    switch (m.t) {
      case 'name': { const n = cleanName(m.name); if (!n) return this.send(p, 'err', { code: 'name' }); p.name = n; this.send(p, 'welcome', { id: p.pid, name: p.name, online: this.onlineCount(), ver: VERSION }); if (r) this.broadcastRoom(r); break; }
      case 'lb': this.send(p, 'lb', this.leaderboard(['day', 'week', 'month'].includes(m.period) ? m.period : 'day', m.kind === 'tour' ? 'tour' : 'solo', p.pid)); break;
      case 'submit': {
        const key = cleanText(m.key, 40); if (key && p.submitted.has(key)) return; if (key) p.submitted.add(key);
        this.addScore(p, 'solo', clampInt(m.score, 0, this.CFG.maxSoloScore), clampInt(m.hits, 0, 9), clampInt(m.shots, 0, 9), DIFFS.includes(m.diff) ? m.diff : 'NORMAL');
        this.send(p, 'submitted', { key }); break;
      }
      case 'rooms': this.send(p, 'rooms', { list: this.openRooms(), online: this.onlineCount() }); break;
      case 'create': {
        if (p.room) this.removeFromRoom(p, 'left');
        this.quickQueue = this.quickQueue.filter(id => id !== p.pid);
        const room = this.newRoom({ name: cleanText(m.name, 24) || `${p.name} Turnuvası`, hostId: p.pid, max: clampInt(m.max, 2, this.CFG.roomMaxLimit), diff: DIFFS.includes(m.diff) ? m.diff : 'NORMAL' });
        room.members.push(p.pid); p.room = room.id; this.broadcastRoom(room); this.roomSys(room, 'created', { name: p.name }); break;
      }
      case 'join': {
        const room = m.code ? [...this.rooms.values()].find(x => x.code === String(m.code).toUpperCase().trim()) : this.rooms.get(m.roomId);
        if (!room || room.quick) return this.send(p, 'err', { code: 'noroom' });
        if (room.members.includes(p.pid)) { p.room = room.id; return this.broadcastRoom(room); }
        if (room.phase !== 'lobby') return this.send(p, 'err', { code: 'started' });
        if (room.members.length >= room.max) return this.send(p, 'err', { code: 'full' });
        if (p.room && p.room !== room.id) this.removeFromRoom(p, 'left');
        this.quickQueue = this.quickQueue.filter(id => id !== p.pid);
        if (!room.pending.includes(p.pid)) room.pending.push(p.pid);
        p.room = room.id;
        this.send(p, 'pending', { name: room.name, code: room.code, host: (this.players.get(room.hostId) || {}).name });
        this.send(this.players.get(room.hostId), 'room', { room: this.roomPublic(room, room.hostId) });
        this.send(this.players.get(room.hostId), 'request', { id: p.pid, name: p.name });
        break;
      }
      case 'approve': case 'reject': {
        if (!r || r.hostId !== p.pid) return this.send(p, 'err', { code: 'nothost' });
        const q = this.players.get(m.id); if (!r.pending.includes(m.id)) return;
        r.pending = r.pending.filter(id => id !== m.id);
        if (m.t === 'approve' && q && r.phase === 'lobby' && r.members.length < r.max) { r.members.push(m.id); q.room = r.id; this.roomSys(r, 'joined', { name: q.name }); }
        else if (q) { q.room = null; this.send(q, 'rejected', { name: r.name, why: m.t === 'reject' ? 'host' : 'full' }); }
        this.broadcastRoom(r); break;
      }
      case 'kick': { if (!r || r.hostId !== p.pid || m.id === p.pid || r.phase === 'playing') return; const q = this.players.get(m.id); if (q && r.members.includes(m.id)) { this.removeFromRoom(q, 'kick'); this.send(q, 'left', { why: 'kick' }); } break; }
      case 'leave': {
        if (r && r.pending.includes(p.pid)) { r.pending = r.pending.filter(id => id !== p.pid); p.room = null; this.send(this.players.get(r.hostId), 'room', { room: this.roomPublic(r, r.hostId) }); }
        else if (r) this.removeFromRoom(p, 'left');
        this.quickQueue = this.quickQueue.filter(id => id !== p.pid); this.send(p, 'left', { why: 'self' }); break;
      }
      case 'start': {
        if (!r || r.hostId !== p.pid) return this.send(p, 'err', { code: 'nothost' });
        if (r.phase === 'playing') return;
        if (r.members.length < 2) return this.send(p, 'err', { code: 'need2' });
        this.startGame(r); break;
      }
      case 'again': { if (!r || r.phase !== 'finished' || (r.hostId && r.hostId !== p.pid)) return; r.phase = 'lobby'; r.order = []; r.results = {}; r.turn = null; this.broadcastRoom(r); break; }
      case 'quick': { if (p.room) this.removeFromRoom(p, 'left'); if (!this.quickQueue.includes(p.pid)) this.quickQueue.push(p.pid); this.quickTick(); break; }
      case 'quickCancel': this.quickQueue = this.quickQueue.filter(id => id !== p.pid); this.send(p, 'quick', { state: 'cancelled' }); break;
      case 'chat': {
        if (!r || !r.members.includes(p.pid)) return;
        const now = Date.now(); if (now - p.lastChat < 600) return; p.lastChat = now;
        const text = cleanText(m.text, 200); if (!text) return;
        const msg = { from: p.pid, name: p.name, text, ts: now }; r.chat.push(msg); if (r.chat.length > 60) r.chat.shift();
        for (const id of r.members) this.send(this.players.get(id), 'chat', msg); break;
      }
      case 'snap': {
        if (!r || !r.turn || r.turn.phase !== 'live' || r.turn.shooter !== p.pid) return;
        const s = JSON.stringify({ t: 'snap', d: m.d, g: r.game, i: r.turn.idx });
        for (const id of r.members) if (id !== p.pid) this.sendRaw(this.players.get(id), s);
        break;
      }
      case 'result': { if (!r || !r.turn || r.turn.phase !== 'live' || r.turn.shooter !== p.pid || m.i !== r.turn.idx) return; this.finishTurn(r, m); break; }
      case 'sync': if (r) this.send(p, 'room', { room: this.roomPublic(r, p.pid), chat: r.chat.slice(-30) }); break;
      case 'ping': this.send(p, 'pong', { ts: m.ts, online: this.onlineCount() }); break;
    }
  }
}
