'use strict'
// CNN-DATA-BOT — пишет мир вокруг игрока как «ключевой кадр + изменения».
// Linux, Node.js >= 18.

const fs = require('fs')
const path = require('path')
const zlib = require('zlib')
const readline = require('readline')
const { execFileSync } = require('child_process')
const { Worker, isMainThread, parentPort } = require('worker_threads')

// ================= ВОРКЕР: gzip и запись на диск =================
function runWorker() {
  const streams = new Map() // `${id}:${kind}` -> { gz, out }
  let chain = Promise.resolve()

  async function handle(m) {
    if (m.type === 'open') {
      const gz = zlib.createGzip({ level: m.level })
      const out = fs.createWriteStream(m.path)
      gz.pipe(out)
      streams.set(`${m.id}:${m.kind}`, { gz, out })
    } else if (m.type === 'frame') {
      const s = streams.get(`${m.id}:${m.kind}`)
      if (!s) return
      const chunk = m.buf ? Buffer.from(m.buf) : Buffer.from(m.text)
      if (!s.gz.write(chunk)) await new Promise(r => s.gz.once('drain', r))
    } else if (m.type === 'close') {
      const keys = [...streams.keys()].filter(k => k.startsWith(m.id + ':'))
      await Promise.all(keys.map(k => new Promise(r => {
        const s = streams.get(k)
        s.out.once('close', r)
        s.gz.end()
        streams.delete(k)
      })))
      parentPort.postMessage({ type: 'closed', id: m.id })
    }
  }

  parentPort.on('message', (m) => {
    chain = chain.then(() => handle(m)).catch(e =>
      parentPort.postMessage({ type: 'error', message: e.message }))
  })
}

if (!isMainThread) { runWorker(); return }

// ================= ОСНОВНОЙ ПОТОК =================
const mineflayer = require('mineflayer')
const PBlock = require('prismarine-block')
const { Vec3 } = require('vec3')

// ---------- конфиг ----------
const DEFAULTS = {
  host: 'localhost',
  port: 25565,
  username: 'CNN_DATA_BOT',
  version: false,          // false = автоопределение
  size: 48,                // сторона куба (±size/2 вокруг бота)
  gzipLevel: 3,            // 1 быстрее, 9 меньше файл
  outDir: 'data',
  controlFifo: 'control.fifo',
  metaEverySec: 10,        // как часто обновлять .json во время записи
  readyTimeoutSec: 15,     // сколько ждать, пока бот прилипнет к игроку
}

function loadConfig() {
  let file = {}
  try {
    file = JSON.parse(fs.readFileSync(path.join(__dirname, 'config.json'), 'utf8'))
  } catch (e) {
    if (e.code !== 'ENOENT') { console.error('[cfg] config.json:', e.message); process.exit(1) }
  }
  const cfg = { ...DEFAULTS, ...file }
  if (process.env.CNN_HOST) cfg.host = process.env.CNN_HOST
  if (process.env.CNN_PORT) cfg.port = Number(process.env.CNN_PORT)
  if (process.env.CNN_USERNAME) cfg.username = process.env.CNN_USERNAME
  if (process.env.CNN_VERSION) cfg.version = process.env.CNN_VERSION
  cfg.outDir = path.resolve(__dirname, cfg.outDir)
  cfg.controlFifo = path.resolve(__dirname, cfg.controlFifo)
  return cfg
}

const CFG = loadConfig()
const S = CFG.size
if (!Number.isInteger(S) || S < 16 || S > 256) {
  console.error('[cfg] size должен быть целым числом 16..256')
  process.exit(1)
}
const HALF = S >> 1
const TICK_MS = 50
const UNKNOWN = 65535

// типы записей в blocks.bin.gz (все little-endian), заголовок 17 байт:
// u8 тип, u32 тик, i32 a, i32 b, i32 c
const R_SECTION = 1 // a,b,c = координаты секции; + 4096 × u16 (индекс (y*16+z)*16+x)
const R_BLOCK = 2   // a,b,c = координаты блока; + u16 stateId
const R_ORIGIN = 3  // a,b,c = угол куба (x0,y0,z0)
const R_DROP = 4    // a,b,c = координаты секции, больше не отслеживается

fs.mkdirSync(CFG.outDir, { recursive: true })

const worker = new Worker(__filename)
const closing = new Map()
worker.on('message', (m) => {
  if (m.type === 'closed') { closing.get(m.id)?.(); closing.delete(m.id) }
  else if (m.type === 'error') console.error('[worker]', m.message)
})
worker.on('error', e => console.error('[worker]', e.message))

const bot = mineflayer.createBot({
  host: CFG.host, port: CFG.port, username: CFG.username, version: CFG.version,
})

let Block = null
let current = null
let wanted = null
let nextId = 1

const r3 = v => Math.round((v ?? 0) * 1000) / 1000
const tmpVec = new Vec3(0, 0, 0)

// ---------- вспомогательное ----------
const blockCache = new Map()
function blockInfo(id) {
  let v = blockCache.get(id)
  if (!v) {
    try {
      const b = Block.fromStateId(id, 0)
      v = { name: b.name, props: b.getProperties() }
    } catch {
      v = { name: 'unknown', props: {} }
    }
    blockCache.set(id, v)
  }
  return v
}

function worldRange() {
  const minY = bot.game?.minY ?? 0
  const height = bot.game?.height ?? 256
  return { minY, height, minSec: Math.floor(minY / 16), maxSec: Math.floor((minY + height - 1) / 16) }
}

function versionInfo() {
  const v = bot.registry?.version || {}
  return { minecraft: bot.version, protocol: v.version, dataVersion: v.dataVersion, majorVersion: v.majorVersion }
}

const curTick = c => Math.floor((Date.now() - c.t0) / TICK_MS)

function rec(type, tick, a, b, c, extra) {
  const buf = Buffer.alloc(17 + extra)
  buf[0] = type
  buf.writeUInt32LE(tick >>> 0, 1)
  buf.writeInt32LE(a, 5)
  buf.writeInt32LE(b, 9)
  buf.writeInt32LE(c, 13)
  return buf
}

function emitSection(c, tick, sx, sy, sz, data) {
  const b = rec(R_SECTION, tick, sx, sy, sz, 4096 * 2)
  Buffer.from(data.buffer, data.byteOffset, 4096 * 2).copy(b, 17)
  c.outBlocks.push(b)
  c.counts.sections++
}

function emitDrop(c, tick, sx, sy, sz) {
  c.outBlocks.push(rec(R_DROP, tick, sx, sy, sz, 0))
  c.counts.drops++
}

function flush(c) {
  if (c.outBlocks.length) {
    let total = 0
    for (const b of c.outBlocks) total += b.length
    const out = new Uint8Array(total)
    let o = 0
    for (const b of c.outBlocks) { out.set(b, o); o += b.length }
    c.outBlocks = []
    worker.postMessage({ type: 'frame', kind: 'blocks', id: c.id, buf: out.buffer }, [out.buffer])
  }
  if (c.outLines.length) {
    worker.postMessage({ type: 'frame', kind: 'entities', id: c.id, text: c.outLines.join('') })
    c.outLines = []
  }
}

// ---------- запись: жизненный цикл ----------
function startRecording(name) {
  stopRecording()
  const ts = new Date().toISOString().replace(/[:.]/g, '-')
  const base = path.join(CFG.outDir, `${name}_${ts}`)
  current = {
    id: nextId++,
    name,
    askedAt: Date.now(),
    t0: null,              // ставится, когда бот прилип к игроку
    startedAt: null,
    blocksPath: base + '.blocks.bin.gz',
    entitiesPath: base + '.entities.jsonl.gz',
    metaPath: base + '.json',
    origin: null,
    tracked: new Set(),    // секции, по которым записан кадр и идут изменения
    pending: new Set(),    // секции нужны, но чанк ещё не загружен
    palette: new Set(),
    entTypes: new Set(),
    items: new Set(),
    lastEnt: new Map(),    // id сущности -> последняя записанная JSON-строка
    lastTarget: undefined,
    entFullDone: false,
    outBlocks: [],
    outLines: [],
    counts: { sections: 0, drops: 0, blockChanges: 0, originMoves: 0, entityLines: 0 },
    lastTick: 0,
    lastMeta: Date.now(),
  }
  worker.postMessage({ type: 'open', kind: 'blocks', id: current.id, path: current.blocksPath, level: CFG.gzipLevel })
  worker.postMessage({ type: 'open', kind: 'entities', id: current.id, path: current.entitiesPath, level: CFG.gzipLevel })
  console.log(`[rec] цель: ${name} -> ${base}.*`)
  sendSpectate()
}

function stopRecording() {
  if (!current) return Promise.resolve()
  const c = current
  current = null
  flush(c)
  writeMeta(c)
  console.log(`[rec] конец записи: ${c.name}, тиков: ${c.lastTick + 1}, секций: ${c.counts.sections}, изменений блоков: ${c.counts.blockChanges}`)
  return new Promise(res => {
    closing.set(c.id, res)
    worker.postMessage({ type: 'close', id: c.id })
  })
}

function writeMeta(c) {
  c.lastMeta = Date.now()
  const palette = {}
  for (const id of c.palette) palette[id] = blockInfo(id)

  const entityTypes = {}
  for (const id of c.entTypes) {
    const e = bot.registry.entities[id]
    entityTypes[id] = e ? { name: e.name, category: e.type, width: e.width, height: e.height } : { name: 'unknown' }
  }
  const items = {}
  for (const id of c.items) items[id] = { name: bot.registry.items[id]?.name ?? 'unknown' }

  const w = worldRange()
  fs.writeFileSync(c.metaPath, JSON.stringify({
    format: 'cnn-data-bot/delta-v1',
    player: c.name,
    startedAt: c.startedAt,
    version: versionInfo(),
    size: S,
    tickMs: TICK_MS,
    ticks: c.lastTick + 1,
    world: { minY: w.minY, height: w.height },
    unknownId: UNKNOWN,
    files: { blocks: path.basename(c.blocksPath), entities: path.basename(c.entitiesPath) },
    compression: 'gzip',
    counts: c.counts,
    palette,      // stateId -> { name, props }
    entityTypes,  // typeId  -> { name, category, width, height }
    items,        // itemId  -> { name }
  }, null, 1))
}

// ---------- блоки: ключевой кадр + изменения ----------
function snapshotSection(c, sx, sy, sz) {
  const col = bot.world.getColumn(sx, sz)
  if (!col) return null
  const out = new Uint16Array(4096)
  const baseY = sy * 16
  let i = 0
  for (let y = 0; y < 16; y++) {
    for (let z = 0; z < 16; z++) {
      for (let x = 0; x < 16; x++) {
        tmpVec.x = x; tmpVec.y = baseY + y; tmpVec.z = z
        const id = col.getBlockStateId(tmpVec)
        out[i++] = id
        c.palette.add(id)
      }
    }
  }
  return out
}

function trySection(c, tick, key) {
  const [sx, sy, sz] = key.split(',').map(Number)
  let data = null
  try { data = snapshotSection(c, sx, sy, sz) } catch (e) { data = null }
  if (!data) { c.pending.add(key); return }
  emitSection(c, tick, sx, sy, sz, data)
  c.tracked.add(key)
  c.pending.delete(key)
}

function updateOrigin(c, tick) {
  const p = bot.entity.position
  const x0 = Math.floor(p.x) - HALF
  const y0 = Math.floor(p.y) - HALF
  const z0 = Math.floor(p.z) - HALF
  if (c.origin && c.origin[0] === x0 && c.origin[1] === y0 && c.origin[2] === z0) return

  c.origin = [x0, y0, z0]
  c.outBlocks.push(rec(R_ORIGIN, tick, x0, y0, z0, 0))
  c.counts.originMoves++

  // какие секции пересекает куб
  const w = worldRange()
  const need = new Set()
  const sy0 = Math.max(w.minSec, y0 >> 4)
  const sy1 = Math.min(w.maxSec, (y0 + S - 1) >> 4)
  for (let sx = x0 >> 4; sx <= (x0 + S - 1) >> 4; sx++) {
    for (let sz = z0 >> 4; sz <= (z0 + S - 1) >> 4; sz++) {
      for (let sy = sy0; sy <= sy1; sy++) need.add(`${sx},${sy},${sz}`)
    }
  }

  for (const key of [...c.tracked]) {
    if (!need.has(key)) {
      const [sx, sy, sz] = key.split(',').map(Number)
      emitDrop(c, tick, sx, sy, sz)
      c.tracked.delete(key)
    }
  }
  for (const key of [...c.pending]) if (!need.has(key)) c.pending.delete(key)
  for (const key of need) {
    if (!c.tracked.has(key) && !c.pending.has(key)) trySection(c, tick, key)
  }
}

bot.on('blockUpdate', (oldBlock, newBlock) => {
  const c = current
  if (!c || !c.t0 || !newBlock) return
  const { x, y, z } = newBlock.position
  if (!c.tracked.has(`${x >> 4},${y >> 4},${z >> 4}`)) return
  if (oldBlock && oldBlock.stateId === newBlock.stateId) return
  const b = rec(R_BLOCK, curTick(c), x, y, z, 2)
  b.writeUInt16LE(newBlock.stateId, 17)
  c.outBlocks.push(b)
  c.palette.add(newBlock.stateId)
  c.counts.blockChanges++
})

bot.on('chunkColumnLoad', (pt) => {
  const c = current
  if (!c || !c.t0) return
  const cx = Math.floor(pt.x / 16), cz = Math.floor(pt.z / 16)
  const tick = curTick(c)
  // сервер мог переслать чанк целиком — пишем свежие секции поверх
  for (const key of c.tracked) {
    const [sx, sy, sz] = key.split(',').map(Number)
    if (sx !== cx || sz !== cz) continue
    try {
      const data = snapshotSection(c, sx, sy, sz)
      if (data) emitSection(c, tick, sx, sy, sz, data)
    } catch {}
  }
})

bot.on('chunkColumnUnload', (pt) => {
  const c = current
  if (!c || !c.t0) return
  const cx = Math.floor(pt.x / 16), cz = Math.floor(pt.z / 16)
  const tick = curTick(c)
  for (const key of [...c.tracked]) {
    const [sx, sy, sz] = key.split(',').map(Number)
    if (sx !== cx || sz !== cz) continue
    emitDrop(c, tick, sx, sy, sz)
    c.tracked.delete(key)
    c.pending.add(key) // вернётся, когда чанк загрузится снова
  }
})

// ---------- сущности: полный список в начале + изменения ----------
function itemRef(c, it) {
  if (!it) return undefined
  c.items.add(it.type)
  return [it.type, it.count]
}

function serializeEntity(c, e) {
  const p = e.position
  const typeId = e.entityType
  if (typeId !== undefined) c.entTypes.add(typeId)
  const r = {
    id: e.id,
    type: typeId ?? null,
    pos: [r3(p.x), r3(p.y), r3(p.z)],
    vel: [r3(e.velocity?.x), r3(e.velocity?.y), r3(e.velocity?.z)],
    yaw: r3(e.yaw),
    pitch: r3(e.pitch),
    head: r3(e.headYaw),
    ground: e.onGround ? 1 : 0,
  }
  if (e.username) r.name = e.username
  if (typeof e.health === 'number') r.hp = e.health
  try {
    if (e.name === 'item' && typeof e.getDroppedItem === 'function') r.item = itemRef(c, e.getDroppedItem())
    if (e.heldItem) r.held = itemRef(c, e.heldItem)
  } catch {}
  return JSON.stringify(r)
}

function sampleEntities(c, tick) {
  const [x0, y0, z0] = c.origin
  const seen = new Set()
  const upd = []
  for (const e of Object.values(bot.entities)) {
    if (e === bot.entity || !e.position) continue
    const p = e.position
    if (p.x < x0 || p.x >= x0 + S || p.y < y0 || p.y >= y0 + S || p.z < z0 || p.z >= z0 + S) continue
    const s = serializeEntity(c, e)
    seen.add(e.id)
    if (c.lastEnt.get(e.id) !== s) {
      c.lastEnt.set(e.id, s)
      upd.push(s)
    }
  }
  const del = []
  for (const id of c.lastEnt.keys()) {
    if (!seen.has(id)) { del.push(id); c.lastEnt.delete(id) }
  }
  const tg = bot.players[c.name]?.entity?.id ?? null
  const tgChanged = tg !== c.lastTarget
  c.lastTarget = tg

  const first = !c.entFullDone
  if (!first && !upd.length && !del.length && !tgChanged) return
  c.entFullDone = true

  let line = `{"t":${tick}`
  if (first) line += ',"full":1'
  if (first || tgChanged) line += `,"tg":${tg}`
  line += `,"upd":[${upd.join(',')}]`
  if (del.length) line += `,"del":${JSON.stringify(del)}`
  line += '}\n'
  c.outLines.push(line)
  c.counts.entityLines++
}

// ---------- главный тик ----------
function targetReady(c) {
  const t = bot.players[c.name]?.entity
  return !!t && bot.entity && t.position.distanceTo(bot.entity.position) < 4
}

setInterval(() => {
  const c = current
  if (!c || !bot.entity) return

  // ждём, пока бот реально прилипнет к игроку, иначе первый кадр будет не там
  if (!c.t0) {
    const timeout = Date.now() - c.askedAt > CFG.readyTimeoutSec * 1000
    if (!targetReady(c) && !timeout) return
    c.t0 = Date.now()
    c.startedAt = new Date().toISOString()
    console.log(`[rec] старт кадра: ${c.name}${timeout ? ' (по таймауту)' : ''}`)
  }

  const tick = curTick(c)
  try {
    updateOrigin(c, tick)
    for (const key of [...c.pending]) trySection(c, tick, key)
    sampleEntities(c, tick)
    c.lastTick = tick
  } catch (e) {
    console.error('[tick]', e.message)
  }
  flush(c)
  if (Date.now() - c.lastMeta > CFG.metaEverySec * 1000) writeMeta(c)
}, TICK_MS)

// ---------- выбор игрока ----------
function onlinePlayers() {
  return Object.keys(bot.players).filter(n => n !== bot.username)
}

function pickNext(preferred) {
  const names = onlinePlayers()
  const target = preferred && names.includes(preferred) ? preferred : names[0]
  if (!target) return console.log('[rec] нет игроков, жду...')
  startRecording(target)
}

function sendSpectate() {
  if (!current) return
  if (bot.game.gameMode !== 'spectator') bot.chat('/gamemode spectator')
  const name = current.name
  setTimeout(() => current && current.name === name && bot.chat(`/spectate ${name}`), 500)
}

// если бот отлип от игрока — повторяем команду
setInterval(() => {
  const c = current
  if (!c || !c.t0 || !bot.entity) return
  const t = bot.players[c.name]?.entity
  if (!t || t.position.distanceTo(bot.entity.position) > 4) sendSpectate()
}, 5000)

async function shutdown() {
  await stopRecording()
  await worker.terminate()
  process.exit(0)
}

// ---------- события ----------
bot.once('spawn', () => {
  Block = PBlock(bot.registry)
  console.log(`[bot] зашёл (Minecraft ${bot.version}). Выдай op: op ${bot.username}`)
  setTimeout(() => pickNext(wanted), 2000)
})

bot.on('playerJoined', () => { if (!current) pickNext(wanted) })

bot.on('playerLeft', (p) => {
  if (current && p.username === current.name) {
    console.log(`[rec] ${p.username} вышел`)
    stopRecording()
    pickNext()
  }
})

bot.on('end', shutdown)
bot.on('kicked', r => console.log('[kick]', r))
bot.on('error', e => console.log('[err]', e.message))

// ---------- управление: терминал и FIFO ----------
function handleCommand(line) {
  const cmd = line.trim()
  if (!cmd) return
  if (cmd === 'list') return console.log(onlinePlayers().join(', ') || '(пусто)')
  if (cmd === 'auto') { wanted = null; return pickNext() }
  if (cmd === 'status') {
    if (!current) return console.log('[status] запись не идёт')
    const c = current
    return console.log(`[status] ${c.name}: тик ${c.lastTick}, секций ${c.tracked.size}, ждут чанк ${c.pending.size}, ${JSON.stringify(c.counts)}`)
  }
  if (cmd === 'quit') { bot.quit(); return }
  if (!onlinePlayers().includes(cmd)) return console.log('[ctl] такого игрока нет онлайн')
  wanted = cmd
  startRecording(cmd)
}

readline.createInterface({ input: process.stdin }).on('line', handleCommand)

// echo Steve > control.fifo  — работает и под systemd, где нет терминала
try {
  if (!fs.existsSync(CFG.controlFifo)) execFileSync('mkfifo', [CFG.controlFifo])
  const fd = fs.openSync(CFG.controlFifo, 'r+') // r+ чтобы не ловить EOF после каждого echo
  readline.createInterface({ input: fs.createReadStream('', { fd, autoClose: false }) }).on('line', handleCommand)
  console.log(`[ctl] FIFO: ${CFG.controlFifo}`)
} catch (e) {
  console.log('[ctl] FIFO недоступен:', e.message)
}

process.on('SIGINT', shutdown)
process.on('SIGTERM', shutdown)
