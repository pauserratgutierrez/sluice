// Live check: drives the built SDK against a running harness THROUGH CADDY.
//
// This is the only test that exercises the whole path a browser would take,
// including the gateway's SSE handling -- `flush_interval -1`, no response
// buffering, CORS headers, and `Vary: Authorization`. A server that streams
// correctly on localhost and buffers behind the proxy is a classic and very
// annoying failure, so it gets its own check.
//
//   node test/live.mjs [baseUrl]
//
// Default base URL is http://localhost:8000.

import { createClient } from '../dist/index.js'

const base = process.argv[2] ?? 'http://localhost:8000'
const authURL = `${base}/auth/v1`
const sluiceURL = `${base}/sluice/v1`
const restURL = `${base}/rest/v1`

let checks = 0
let failures = 0
const check = (ok, what, detail = '') => {
  checks++
  if (ok) return console.log(`  PASS  ${what}`)
  failures++
  console.log(`  FAIL  ${what}`)
  if (detail) console.log(`        ${detail}`)
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms))

async function signUp() {
  const email = `sdk-${Date.now()}@example.test`
  const res = await fetch(`${authURL}/signup`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ email, password: 'sluice-sdk-password-1' }),
  })
  if (!res.ok) throw new Error(`signup failed: ${res.status} ${await res.text()}`)
  const body = await res.json()
  return { token: body.access_token, userId: body.user.id }
}

console.log('== sluice-js live check (through the gateway) ==\n')

const { token, userId } = await signUp()
console.log(`user  ${userId}\n`)

const statuses = []
const warnings = []

const client = createClient(sluiceURL, {
  accessToken: () => token,
  pauseWhenHidden: false,
  onStatusChange: (s) => statuses.push(s),
  onWarning: (w) => warnings.push(w.code),
  onError: (e) => console.log(`  note  stream error: ${e.code} ${e.message}`),
})

// ---- table subscription ----------------------------------------------------
const received = []
const docs = await client
  .from('documents')
  .as('docs')
  .eq('owner_id', userId)
  .select('id', 'title')
  .withTransitions()
  .on('*', (c) => received.push(c))
  .subscribe()

check(docs.ok, 'subscribed to a table through the gateway', JSON.stringify(docs.error ?? {}))
check(docs.tier === 'A', `the shape resolved to Tier A (got ${docs.tier})`, docs.reason)
check(docs.indexed === true, 'the shape is indexed by its filter constant', `routing key: ${docs.routingKey}`)
check(statuses.includes('open'), 'the client reported an open connection', statuses.join(' -> '))

// ---- live change -----------------------------------------------------------
async function insertDoc(title, owner = userId) {
  const res = await fetch(`${restURL}/documents`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      Authorization: `Bearer ${token}`,
      Prefer: 'return=minimal',
    },
    body: JSON.stringify({ owner_id: owner, title }),
  })
  if (!res.ok) throw new Error(`insert failed: ${res.status} ${await res.text()}`)
}

await insertDoc('from the sdk')
await sleep(1500)

check(received.length === 1, 'the change arrived over SSE through Caddy', `received ${received.length}`)
if (received[0]) {
  const c = received[0]
  check(c.op === 'INSERT' && c.table === 'documents', 'the event carries op and table', JSON.stringify(c).slice(0, 120))
  check(c.record?.title === 'from the sdk', 'the projected record has the selected columns', JSON.stringify(c.record))
  check(c.record?.body === undefined, 'unselected columns are absent from the projection', JSON.stringify(c.record))
}

// Latency: if Caddy were buffering, this would be seconds, not milliseconds.
const t0 = Date.now()
const before = received.length
await insertDoc('latency probe')
for (let i = 0; i < 100 && received.length === before; i++) await sleep(20)
const latency = Date.now() - t0
check(received.length > before, 'a second change arrived', '')
check(latency < 1000, `end-to-end latency is under 1s (${latency}ms)`, 'a multi-second value means the proxy is buffering')

// ---- channel: broadcast and presence --------------------------------------
const room = client.channel('room:sdk')
const messages = []
const presenceStates = []
room.on('cursor', (p) => messages.push(p))
room.onPresence((state) => presenceStates.push(Object.keys(state)))

const joined = await room.subscribe()
check(joined.ok, 'joined a public channel', JSON.stringify(joined.error ?? {}))

await room.track({ name: 'sdk' })
const delivered = await room.send('cursor', { x: 1, y: 2 }, true)
await sleep(300)

check(delivered >= 1, 'send reported how many streams it reached', `delivered=${delivered}`)
check(messages.length === 1 && messages[0].x === 1, 'the broadcast came back with self=true', JSON.stringify(messages))
await sleep(2000)
check(presenceStates.some((keys) => keys.includes(userId)), 'presence reported this client as a member',
  JSON.stringify(presenceStates))

// ---- send before subscribe is refused client-side --------------------------
try {
  await client.channel('room:not-joined').send('x', {})
  check(false, 'sending to an unjoined channel throws', 'it did not throw')
} catch {
  check(true, 'sending to an unjoined channel throws', '')
}

// ---- unsubscribe stops delivery -------------------------------------------
await docs.unsubscribe()
const afterUnsub = received.length
await insertDoc('after unsubscribe')
await sleep(1200)
check(received.length === afterUnsub, 'no events arrive after unsubscribe',
  `received ${received.length - afterUnsub} extra`)

client.close()
await sleep(200)

console.log(`\n== ${checks} checks, ${failures} failures ==`)
if (warnings.length) console.log(`   server warnings seen: ${[...new Set(warnings)].join(', ')}`)
process.exit(failures > 0 ? 1 : 0)
