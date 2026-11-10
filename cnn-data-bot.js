// recorder.js — npm i mineflayer vec3 prismarine-block
const mineflayer = require('mineflayer')
const PBlock = require('prismarine-block')
const { Vec3 } = require('vec3')
const readline = require('readline')
const fs = require('fs')
const path = require('path')
const zlib = require('zlib')
const { Worker, isMainThread, parentPort } = require('worker_threads')

// ================= ВОРКЕР: сжатие и запись =================
function runWorker() {
  const streams = new Map()
  let chain = Promise.resolve()

  async function handle(m) {
    if (m.type === 'open') {
      const gz = zlib.createGzip({ level: m.level })
      const out = fs.createWriteStream(m.path)
      gz.pipe(out)
      streams.set(m.id, { gz, out })
    } else if (m.type === 'frame') {
      const s = streams.get(m.id)
      if (!s) return
      if (!s.gz.write(Buffer.from(m.buf))) {
        await new Promise(r => s.gz.once('drain', r))
      }
    } else if (m.type === 'close') {
      const s = streams.get(m.id)
      if (s) {
        await new Promise(r => { s.out.once('close', r); s.gz.end() })
        streams.delete(m.id)
      }
      parentPort.postMessage({ type: 'closed', id: m.id })
    }
  }

  // сообщения обрабатываются строго по очереди
  parentPort.on('message', (m) => {
    chain = chain.then(() => handle(m)).catch(e =>
      parentPort.postMessage({ type: 'error', message: e.message }))
  })
}

if (!isMainThread) { runWorker(); return }

// ================= ОСНОВНОЙ ПОТОК =================
const CFG = {
  host: 'localhost',
  port: 25565,
  username: 'Recorder',
  version: false,        // false = автоопределение
  size: 48,              // сторона куба (±24 вокруг бота)
  everyTicks: 20,        // снимок раз в N тиков (20 = 1 раз/сек)
  gzipLevel: 3,          // 1 = быстрее, 9 = меньше файл
  outDir: 'data',
}

const S = CFG.size
const HALF = S / 2
const UNKNOWN = 65535

const worker = new Worker(__filename)
const closing = new Map() // id -> resolve
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

fs.mkdirSync(CFG.outDir, { recursive: true })

// ---------- запись ----------
function startRecording(name) {
  stopRecording()
  const ts = new Date().toISOString().replace(/[:.]/g, '-')
  const base = path.join(CFG.outDir, `${name}_${ts}`)
  current = {
    id: nextId++,
    name,
    binPath: base + '.bin.gz',
    metaPath: base + '.json',
    frames: 0,
    palette: new Map(),
    startedAt: new Date().toISOString(),
  }
  worker.postMessage({ type: 'open', id: current.id, path: current.binPath, level: CFG.gzipLevel })
  console.log(`[rec] старт записи: ${name} -> ${current.binPath}`)
  sendSpectate()
}

function stopRecording() {
  if (!current) return Promise.resolve()
  const c = current
  current = null
  writeMeta(c)
  console.log(`[rec] конец записи: ${c.name}, кадров: ${c.frames}`)
  return new Promise(res => {
    closing.set(c.id, res)
    worker.postMessage({ type: 'close', id: c.id })
  })
}

function writeMeta(c) {
  const palette = {}
  for (const [id, info] of c.palette) palette[id] = info
  fs.writeFileSync(c.metaPath, JSON.stringify({
    player: c.name,
    startedAt: c.startedAt,
    size: S,
    unknownId: UNKNOWN,
    frames: c.frames,
    compression: 'gzip',
    // после распаковки кадр: u32 номер, i32 x0, i32 y0, i32 z0, затем S*S*S значений u16 LE
    // индекс блока: (dy*S + dz)*S + dx
    frameHeaderBytes: 16,
    frameDataBytes: S * S * S * 2,
    palette, // stateId -> { name, props }
  }, null, 1))
}

function paletteAdd(c, id) {
  if (c.palette.has(id)) return
  try {
    const b = Block.fromStateId(id, 0)
    c.palette.set(id, { name: b.name, props: b.getProperties() })
  } catch {
    c.palette.set(id, { name: 'unknown', props: {} })
  }
}

function snapshot() {
  const c = current
  if (!c || !bot.entity) return
  try {
    const p = bot.entity.position.floored()
    const x0 = p.x - HALF, y0 = p.y - HALF, z0 = p.z - HALF
    const minY = bot.game.minY ?? -64
    const maxY = minY + (bot.game.height ?? 384) - 1

    // кадр собираем сразу в ArrayBuffer, чтобы передать воркеру без копирования
    const ab = new ArrayBuffer(16 + S * S * S * 2)
    const dv = new DataView(ab)
    dv.setUint32(0, c.frames, true)
    dv.setInt32(4, x0, true)
    dv.setInt32(8, y0, true)
    dv.setInt32(12, z0, true)
    const data = new Uint16Array(ab, 16)

    const tmp = new Vec3(0, 0, 0)
    const seen = new Set()

    for (let dx0 = 0; dx0 < S; ) {
      const wx = x0 + dx0
      const lx = ((wx % 16) + 16) % 16
      const runX = Math.min(16 - lx, S - dx0)
      for (let dz0 = 0; dz0 < S; ) {
        const wz = z0 + dz0
        const lz = ((wz % 16) + 16) % 16
        const runZ = Math.min(16 - lz, S - dz0)
        const col = bot.world.getColumn(Math.floor(wx / 16), Math.floor(wz / 16))

        for (let dy = 0; dy < S; dy++) {
          const wy = y0 + dy
          const outOfWorld = wy < minY || wy > maxY
          for (let iz = 0; iz < runZ; iz++) {
            for (let ix = 0; ix < runX; ix++) {
              let id
              if (outOfWorld) id = 0
              else if (!col) id = UNKNOWN
              else {
                tmp.x = lx + ix; tmp.y = wy; tmp.z = lz + iz
                id = col.getBlockStateId(tmp)
              }
              data[(dy * S + dz0 + iz) * S + dx0 + ix] = id
              seen.add(id)
            }
          }
        }
        dz0 += runZ
      }
      dx0 += runX
    }

    for (const id of seen) if (id !== UNKNOWN) paletteAdd(c, id)

    worker.postMessage({ type: 'frame', id: c.id, buf: ab }, [ab])
    c.frames++
    if (c.frames % 30 === 0) writeMeta(c)
  } catch (e) {
    console.error('[snapshot]', e.message)
  }
}

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

setInterval(() => {
  if (!current || !bot.entity) return
  const t = bot.players[current.name]?.entity
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
  console.log(`[bot] зашёл. Выдай op:  /op ${bot.username}`)
  setTimeout(() => pickNext(wanted), 2000)
  setInterval(snapshot, CFG.everyTicks * 50)
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

// ---------- терминал ----------
const rl = readline.createInterface({ input: process.stdin })
rl.on('line', (line) => {
  const cmd = line.trim()
  if (!cmd) return
  if (cmd === 'list') return console.log(onlinePlayers().join(', ') || '(пусто)')
  if (cmd === 'auto') { wanted = null; return pickNext() }
  if (cmd === 'quit') { bot.quit(); return }
  if (!onlinePlayers().includes(cmd)) return console.log('[term] такого игрока нет онлайн')
  wanted = cmd
  startRecording(cmd)
})

process.on('SIGINT', shutdown)