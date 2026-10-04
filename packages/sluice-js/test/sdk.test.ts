import assert from 'node:assert/strict'
import { test } from 'node:test'

// Tests run against the BUILT output, so what is verified is what ships.
import { createClient, parseSSE, type ChangePayload } from '../dist/index.js'

// ---------------------------------------------------------------------------
// A representative generated schema, structurally identical to what
// supabase/postgres-meta emits.
// ---------------------------------------------------------------------------
type Database = {
  public: {
    Tables: {
      documents: {
        Row: { id: number; owner_id: string; title: string; body: string | null; archived: boolean }
      }
      metrics: {
        Row: { id: number; name: string; value: number }
      }
    }
  }
  analytics: {
    Tables: {
      events: { Row: { id: number; kind: string } }
    }
  }
}

function stream(chunks: string[]): ReadableStream<Uint8Array> {
  const encoder = new TextEncoder()
  return new ReadableStream({
    start(controller) {
      for (const c of chunks) controller.enqueue(encoder.encode(c))
      controller.close()
    },
  })
}

async function collect(chunks: string[]) {
  const out = []
  for await (const ev of parseSSE(stream(chunks))) out.push(ev)
  return out
}

// ---------------------------------------------------------------------------
// SSE framing
// ---------------------------------------------------------------------------

test('parses a simple event', async () => {
  const got = await collect(['event: change\ndata: {"a":1}\n\n'])
  assert.deepEqual(got, [{ event: 'change', id: undefined, data: '{"a":1}' }])
})

test('a blank line dispatches, and an unterminated event is discarded', async () => {
  // The WHATWG grammar is explicit: an event without its terminating blank line
  // is not dispatched. Delivering a truncated final event would be a silent
  // correctness bug.
  const got = await collect(['event: a\ndata: 1\n\nevent: b\ndata: 2\n'])
  assert.equal(got.length, 1)
  assert.equal(got[0]!.event, 'a')
})

test('ignores comments, which is how the heartbeat arrives', async () => {
  const got = await collect([': hb\n\nevent: change\ndata: x\n\n: hb\n\n'])
  assert.equal(got.length, 1)
  assert.equal(got[0]!.data, 'x')
})

test('handles events split across chunk boundaries', async () => {
  // A network read can land anywhere, including mid-field and mid-UTF-8.
  const got = await collect(['event: chan', 'ge\nda', 'ta: {"t":"caf', 'é"}\n', '\n'])
  assert.equal(got.length, 1)
  assert.equal(got[0]!.event, 'change')
  assert.equal(got[0]!.data, '{"t":"café"}')
})

test('joins multiple data lines with a newline', async () => {
  const got = await collect(['data: line1\ndata: line2\n\n'])
  assert.equal(got[0]!.data, 'line1\nline2')
})

test('accepts CR, LF and CRLF terminators', async () => {
  for (const nl of ['\n', '\r\n', '\r']) {
    const got = await collect([`event: e${nl}data: d${nl}${nl}`])
    assert.equal(got.length, 1, `terminator ${JSON.stringify(nl)}`)
    assert.equal(got[0]!.data, 'd')
  }
})

test('strips exactly one leading space after the colon', async () => {
  const got = await collect(['data:  two spaces\n\n'])
  assert.equal(got[0]!.data, ' two spaces')
})

test('captures the id field for resume', async () => {
  const got = await collect(['event: change\nid: 1A2B/3C4D:7\ndata: {}\n\n'])
  assert.equal(got[0]!.id, '1A2B/3C4D:7')
})

// ---------------------------------------------------------------------------
// Client behaviour
// ---------------------------------------------------------------------------

test('sends the token in an Authorization header, never in the URL', async () => {
  let seenUrl = ''
  let seenAuth = ''
  const client = createClient<Database>('https://example.test/sluice/v1', {
    accessToken: 'tok-123',
    pauseWhenHidden: false,
    fetch: async (input, init) => {
      seenUrl = String(input)
      seenAuth = new Headers(init?.headers).get('Authorization') ?? ''
      return new Response('event: ready\ndata: {"stream_id":"n1.x","subscriptions":[{"sub":"s1","ok":true,"tier":"A"}]}\n\n', {
        status: 200,
        headers: { 'Content-Type': 'text/event-stream' },
      })
    },
  })

  await client.from('documents').as('s1').eq('owner_id', 'u1').on('*', () => {}).subscribe()

  assert.equal(seenAuth, 'Bearer tok-123')
  assert.ok(!seenUrl.includes('tok-123'), 'the token must never appear in the URL')
  assert.ok(!seenUrl.includes('apikey'), 'there is no apikey query parameter')
  client.close()
})

/** A fetch that answers /stream the way the server does: one result per subscription sent. */
function echoFetch(onBody?: (body: any) => void): typeof fetch {
  return async (_input, init) => {
    const body = JSON.parse(String(init?.body))
    onBody?.(body)
    const results = body.subscriptions.map((s: { sub: string }) => ({ sub: s.sub, ok: true }))
    return new Response('event: ready\ndata: ' + JSON.stringify({ stream_id: 'n1.x', subscriptions: results }) + '\n\n', {
      status: 200,
      headers: { 'Content-Type': 'text/event-stream' },
    })
  }
}

test('builds a PostgREST-shaped filter string', async () => {
  let body: any
  const client = createClient<Database>('https://example.test/sluice/v1', {
    accessToken: 'tok',
    pauseWhenHidden: false,
    fetch: echoFetch((b) => (body = b)),
  })

  await client
    .from('documents')
    .as('s1')
    .eq('owner_id', 'u1')
    .gte('id', 10)
    .in('title', ['a', 'b'])
    .notEq('archived', true)
    .is('body', null)
    .select('id', 'title')
    .ops('INSERT', 'UPDATE')
    .withTransitions()
    .withInitialSnapshot()
    .on('*', () => {})
    .subscribe()

  const shape = body.subscriptions[0].shape
  assert.equal(shape.table, 'documents')
  assert.equal(shape.schema, 'public')
  assert.equal(shape.filter, 'owner_id=eq.u1,id=gte.10,title=in.(a,b),archived=not.eq.true,body=is.null')
  assert.deepEqual(shape.columns, ['id', 'title'])
  assert.deepEqual(shape.ops, ['INSERT', 'UPDATE'])
  assert.equal(shape.transitions, true)
  assert.equal(shape.initial, 'snapshot')
  client.close()
})

test('surfaces the tier and warnings the server reported', async () => {
  const client = createClient<Database>('https://example.test/sluice/v1', {
    accessToken: 'tok',
    pauseWhenHidden: false,
    fetch: async () =>
      new Response(
        'event: ready\ndata: ' +
          JSON.stringify({
            stream_id: 'n1.x',
            subscriptions: [
              {
                sub: 's1',
                ok: true,
                tier: 'C',
                indexed: false,
                reason: 'contains a subquery',
                warnings: [{ code: 'unindexed_shape', message: 'no equality filter', remedy: 'add one' }],
              },
            ],
          }) +
          '\n\n',
        { status: 200, headers: { 'Content-Type': 'text/event-stream' } },
      ),
  })

  const warnings: string[] = []
  const sub = await client
    .from('documents')
    .as('s1')
    .onWarning((w) => warnings.push(w.code))
    .on('*', () => {})
    .subscribe()

  assert.equal(sub.tier, 'C')
  assert.equal(sub.indexed, false)
  assert.match(sub.reason ?? '', /subquery/)
  assert.deepEqual(warnings, ['unindexed_shape'])
  assert.equal(sub.warnings[0]?.remedy, 'add one')
  client.close()
})

test('issuer ready carries oracle and filter, not a fake tier', async () => {
  const client = createClient<Database>('https://example.test/sluice/v1', {
    accessToken: 'tok',
    pauseWhenHidden: false,
    fetch: async () =>
      new Response(
        'event: ready\ndata: ' +
          JSON.stringify({
            stream_id: 'n1.x',
            subscriptions: [
              {
                sub: 's1',
                ok: true,
                oracle: 'issuer',
                filter: 'project_id=eq.42',
                indexed: true,
                routing_key: 'project_id',
              },
            ],
          }) +
          '\n\n',
        { status: 200, headers: { 'Content-Type': 'text/event-stream' } },
      ),
  })

  const sub = await client.from('documents').as('s1').on('*', () => {}).subscribe()
  assert.equal(sub.oracle, 'issuer')
  assert.equal(sub.filter, 'project_id=eq.42')
  assert.equal(sub.tier, undefined)
  client.close()
})

test('routes change events to the right subscription and applies handlers by op', async () => {
  const change = {
    sub: 's1',
    op: 'UPDATE',
    schema: 'public',
    table: 'documents',
    commit_lsn: '0/1',
    seq: 1,
    record: { id: 1, title: 'x' },
    unchanged: ['body'],
  }
  const client = createClient<Database>('https://example.test/sluice/v1', {
    accessToken: 'tok',
    pauseWhenHidden: false,
    // Held open: a stream that ends reconnects and would replay the change.
    fetch: async () =>
      sse(
        openStream(
          'event: ready\ndata: {"stream_id":"n1.x","subscriptions":[{"sub":"s1","ok":true}]}\n\n' +
            'event: change\ndata: ' + JSON.stringify(change) + '\n\n',
        ),
      ),
  })

  const seen: string[] = []
  let unchangedSeen: string[] | undefined
  await client
    .from('documents')
    .as('s1')
    .select('id', 'title')
    .on('INSERT', () => seen.push('insert'))
    .on('UPDATE', (c) => {
      seen.push('update')
      unchangedSeen = c.unchanged
      // Type-level: the projection narrowed the row.
      const t: string | undefined = c.record?.title
      void t
    })
    .subscribe()

  await new Promise((r) => setTimeout(r, 50))
  assert.deepEqual(seen, ['update'], 'only the matching op handler runs')
  assert.deepEqual(unchangedSeen, ['body'], 'unchanged TOAST columns are surfaced, not silently missing')
  client.close()
})

test('a duplicate subscription label is rejected', async () => {
  const client = createClient<Database>('https://example.test/sluice/v1', {
    accessToken: 'tok',
    pauseWhenHidden: false,
    fetch: echoFetch(),
  })
  await client.from('documents').as('dup').on('*', () => {}).subscribe()
  await assert.rejects(
    () => client.from('metrics').as('dup').on('*', () => {}).subscribe(),
    /already exists/,
  )
  client.close()
})

test('a non-retryable HTTP status does not spin in a reconnect loop', async () => {
  let calls = 0
  const client = createClient<Database>('https://example.test/sluice/v1', {
    accessToken: 'tok',
    pauseWhenHidden: false,
    backoff: [1],
    fetch: async () => {
      calls++
      return new Response('{"error":"unauthorized"}', { status: 401 })
    },
  })
  await client.from('documents').as('s1').on('*', () => {}).subscribe().catch(() => {})
  await new Promise((r) => setTimeout(r, 60))
  assert.ok(calls <= 2, `401 should not be retried, saw ${calls} attempts`)
  client.close()
})

/** An SSE body that stays open until the client aborts it. */
function openStream(first: string): ReadableStream<Uint8Array> {
  const encoder = new TextEncoder()
  return new ReadableStream({
    start(controller) {
      controller.enqueue(encoder.encode(first))
    },
  })
}

const sse = (body: ReadableStream<Uint8Array> | string) =>
  new Response(body, { status: 200, headers: { 'Content-Type': 'text/event-stream' } })

test('a subscription registered after the stream request was sent is subscribed separately', async () => {
  const subscribed: string[] = []
  let streamOpened!: () => void
  const opened = new Promise<void>((r) => (streamOpened = r))
  let release!: () => void
  const released = new Promise<void>((r) => (release = r))
  const client = createClient<Database>('https://example.test/sluice/v1', {
    accessToken: 'tok',
    pauseWhenHidden: false,
    fetch: async (input, init) => {
      const body = JSON.parse(String(init?.body))
      if (String(input).endsWith('/subscribe')) {
        subscribed.push(body.subscriptions[0].sub)
        return new Response(JSON.stringify({ results: [{ sub: body.subscriptions[0].sub, ok: true }] }))
      }
      // The stream request carries only what was registered when it was sent.
      streamOpened()
      await released
      const results = body.subscriptions.map((s: { sub: string }) => ({ sub: s.sub, ok: true }))
      return sse(openStream('event: ready\ndata: ' + JSON.stringify({ stream_id: 'n1.x', subscriptions: results }) + '\n\n'))
    },
  })

  const first = client.from('documents').as('a').on('*', () => {}).subscribe()
  await opened
  const second = client.from('metrics').as('b').on('*', () => {}).subscribe()
  release()
  const [a, b] = await Promise.all([first, second])

  assert.ok(a.ok && b.ok)
  assert.deepEqual(subscribed, ['b'], 'the late subscription must be sent with /subscribe')
  client.close()
})

test('a failed subscribe leaves nothing registered, so the same label can be retried', async () => {
  const calls: string[] = []
  let failNext = true
  const client = createClient<Database>('https://example.test/sluice/v1', {
    accessToken: 'tok',
    pauseWhenHidden: false,
    fetch: async (input, init) => {
      const path = String(input).replace('https://example.test/sluice/v1', '')
      const body = JSON.parse(String(init?.body))
      if (path === '/stream') {
        const results = body.subscriptions.map((s: { sub: string }) => ({ sub: s.sub, ok: true }))
        return sse(openStream('event: ready\ndata: ' + JSON.stringify({ stream_id: 'n1.x', subscriptions: results }) + '\n\n'))
      }
      calls.push(`${path} ${(body.subscriptions?.[0]?.sub ?? body.subs?.[0]) as string}`)
      if (path === '/subscribe' && failNext) {
        failNext = false
        throw new TypeError('network down')
      }
      if (path === '/subscribe') return new Response(JSON.stringify({ results: [{ sub: body.subscriptions[0].sub, ok: true }] }))
      return new Response(JSON.stringify({ removed: 0 }))
    },
  })

  await client.from('documents').as('a').on('*', () => {}).subscribe()
  await assert.rejects(() => client.from('metrics').as('b').on('*', () => {}).subscribe(), /network down/)
  const retry = await client.from('metrics').as('b').on('*', () => {}).subscribe()

  assert.equal(retry.ok, true)
  assert.deepEqual(calls, ['/subscribe b', '/unsubscribe b', '/subscribe b'])
  client.close()
})

test('subscriptions made in the same tick are sent in one /subscribe', async () => {
  const subscribes: string[][] = []
  const client = createClient<Database>('https://example.test/sluice/v1', {
    accessToken: 'tok',
    pauseWhenHidden: false,
    fetch: async (input, init) => {
      const body = JSON.parse(String(init?.body))
      if (String(input).endsWith('/subscribe')) {
        const subs = body.subscriptions.map((s: { sub: string }) => s.sub)
        subscribes.push(subs)
        // Results may come back in any order; each is matched by its label.
        const results = [...subs].reverse().map((sub: string) => ({ sub, ok: sub !== 'c' }))
        return new Response(JSON.stringify({ results }))
      }
      const results = body.subscriptions.map((s: { sub: string }) => ({ sub: s.sub, ok: true }))
      return sse(openStream('event: ready\ndata: ' + JSON.stringify({ stream_id: 'n1.x', subscriptions: results }) + '\n\n'))
    },
  })

  await client.from('documents').as('first').on('*', () => {}).subscribe()
  const [a, b, c] = await Promise.all([
    client.from('documents').as('a').on('*', () => {}).subscribe(),
    client.from('metrics').as('b').on('*', () => {}).subscribe(),
    client.from('metrics').as('c').on('*', () => {}).onError(() => {}).subscribe(),
  ])

  assert.deepEqual(subscribes, [['a', 'b', 'c']])
  assert.equal(a.sub, 'a')
  assert.ok(a.ok && b.ok)
  assert.equal(c.ok, false)
  client.close()
})

test('a shutdown error sets the delay before the reconnect', async () => {
  const opened: number[] = []
  const client = createClient<Database>('https://example.test/sluice/v1', {
    accessToken: 'tok',
    pauseWhenHidden: false,
    backoff: [1],
    onError: () => {},
    fetch: async () => {
      opened.push(Date.now())
      const ready = 'event: ready\ndata: {"stream_id":"n1.x","subscriptions":[{"sub":"s1","ok":true}]}\n\n'
      if (opened.length === 1) {
        return sse(ready + 'event: error\ndata: {"code":"server_shutdown","message":"bye","retryable":true,"retry_after_ms":120}\n\n')
      }
      return sse(openStream(ready))
    },
  })
  await client.from('documents').as('s1').on('*', () => {}).subscribe()
  await new Promise((r) => setTimeout(r, 250))
  assert.equal(opened.length, 2)
  assert.ok(opened[1]! - opened[0]! >= 110, `reconnected after ${opened[1]! - opened[0]!} ms, want the 120 ms the server asked for`)
  client.close()
})

test('resume_too_old forgets the position instead of resending it', async () => {
  const resumes: Record<string, string>[] = []
  const change = { sub: 's1', op: 'INSERT', schema: 'public', table: 'documents', commit_lsn: '0/10', seq: 1, record: { id: 1 } }
  const client = createClient<Database>('https://example.test/sluice/v1', {
    accessToken: 'tok',
    pauseWhenHidden: false,
    backoff: [1],
    fetch: async (_input, init) => {
      resumes.push(JSON.parse(String(init?.body)).resume)
      const ready = 'event: ready\ndata: {"stream_id":"n1.x","subscriptions":[{"sub":"s1","ok":true}]}\n\n'
      if (resumes.length === 1) return sse(ready + 'event: change\ndata: ' + JSON.stringify(change) + '\n\n')
      if (resumes.length === 2) {
        return sse(ready + 'event: error\ndata: {"sub":"s1","code":"resume_too_old","message":"gone","retryable":true,"action":"resnapshot"}\n\n')
      }
      return sse(openStream(ready))
    },
  })

  await client.from('documents').as('s1').on('*', () => {}).onError(() => {}).subscribe()
  await new Promise((r) => setTimeout(r, 80))
  assert.ok(resumes.length >= 3, `expected three stream requests, saw ${resumes.length}`)
  assert.deepEqual(resumes[1], { 'public.documents': '0/10' })
  assert.deepEqual(resumes[2], {}, 'the position the server no longer has must not be resent')
  client.close()
})

test('a refused subscription is not resent on reconnect', async () => {
  let calls = 0
  const client = createClient<Database>('https://example.test/sluice/v1', {
    accessToken: 'tok',
    pauseWhenHidden: false,
    backoff: [1],
    fetch: async () => {
      calls++
      return sse(
        'event: ready\ndata: ' +
          JSON.stringify({
            stream_id: 'n1.x',
            subscriptions: [{ sub: 's1', ok: false, error: { code: 'shape_not_authorized', message: 'no' } }],
          }) +
          '\n\n',
      )
    },
  })
  const sub = await client.from('documents').as('s1').on('*', () => {}).onError(() => {}).subscribe()
  await new Promise((r) => setTimeout(r, 40))
  assert.equal(sub.ok, false)
  assert.equal(calls, 1, 'with nothing left to subscribe, the client must not reconnect')
  client.close()
})

/** An SSE body that stays open, with events pushed by the test. */
function pushStream() {
  const encoder = new TextEncoder()
  let ctrl!: ReadableStreamDefaultController<Uint8Array>
  const body = new ReadableStream<Uint8Array>({ start: (c) => void (ctrl = c) })
  return { body, push: (s: string) => ctrl.enqueue(encoder.encode(s)) }
}

test('a channel the server removes reports the error, refuses send, and can be rejoined', async () => {
  const calls: string[] = []
  const live = pushStream()
  let label = ''
  const client = createClient<Database>('https://example.test/sluice/v1', {
    accessToken: 'tok',
    pauseWhenHidden: false,
    fetch: async (input, init) => {
      const path = String(input).replace('https://example.test/sluice/v1', '')
      const body = JSON.parse(String(init?.body))
      calls.push(path)
      if (path === '/stream') {
        label = body.subscriptions[0].sub
        live.push('event: ready\ndata: ' + JSON.stringify({ stream_id: 'n1.x', subscriptions: [{ sub: label, ok: true }] }) + '\n\n')
        return sse(live.body)
      }
      if (path === '/subscribe') return new Response(JSON.stringify({ results: [{ sub: body.subscriptions[0].sub, ok: true }] }))
      if (path === '/publish') return new Response(JSON.stringify({ delivered: 1 }))
      return new Response(JSON.stringify({ removed: 0 }))
    },
  })

  const channel = client.channel('chat:1')
  const errors: string[] = []
  let rejoined: Promise<{ ok: boolean }> | undefined
  channel.onError((e) => {
    errors.push(e.code)
    // Rejoining from the handler itself must work: the removed join is
    // already forgotten.
    rejoined = channel.subscribe()
  })
  assert.equal((await channel.subscribe()).ok, true)
  assert.equal(await channel.send('typing', {}), 1)

  live.push('event: error\ndata: ' + JSON.stringify({ sub: label, code: 'channel_not_authorized', message: 'blocked' }) + '\n\n')
  await new Promise((r) => setTimeout(r, 20))
  assert.deepEqual(errors, ['channel_not_authorized'])
  assert.ok(rejoined, 'onError did not run')
  assert.equal((await rejoined).ok, true)
  assert.equal(await channel.send('typing', {}), 1)
  assert.deepEqual(calls, ['/stream', '/publish', '/subscribe', '/publish'])
  client.close()
})

test('a removed channel refuses send until it is joined again', async () => {
  const live = pushStream()
  let label = ''
  const client = createClient<Database>('https://example.test/sluice/v1', {
    accessToken: 'tok',
    pauseWhenHidden: false,
    fetch: async (_input, init) => {
      label = JSON.parse(String(init?.body)).subscriptions?.[0]?.sub ?? label
      live.push('event: ready\ndata: ' + JSON.stringify({ stream_id: 'n1.x', subscriptions: [{ sub: label, ok: true }] }) + '\n\n')
      return sse(live.body)
    },
  })
  const channel = client.channel('chat:1')
  const errors: string[] = []
  channel.onError((e) => errors.push(e.code))
  await channel.subscribe()
  live.push('event: error\ndata: ' + JSON.stringify({ sub: label, code: 'channel_not_authorized', message: 'blocked' }) + '\n\n')
  await new Promise((r) => setTimeout(r, 20))
  assert.deepEqual(errors, ['channel_not_authorized'])
  await assert.rejects(() => channel.send('typing', {}), /subscribe/)
  client.close()
})

test('a channel refused when a reconnect resends it reports the error', async () => {
  let streams = 0
  let label = ''
  const client = createClient<Database>('https://example.test/sluice/v1', {
    accessToken: 'tok',
    pauseWhenHidden: false,
    backoff: [1],
    fetch: async (_input, init) => {
      label = JSON.parse(String(init?.body)).subscriptions[0].sub
      streams++
      const result = streams === 1
        ? { sub: label, ok: true }
        : { sub: label, ok: false, error: { code: 'channel_not_authorized', message: 'blocked' } }
      const ready = 'event: ready\ndata: ' + JSON.stringify({ stream_id: 'n1.x', subscriptions: [result] }) + '\n\n'
      // The first stream ends, so the client reconnects and resends the join.
      return sse(streams === 1 ? ready : openStream(ready))
    },
  })
  const channel = client.channel('chat:1')
  const errors: string[] = []
  channel.onError((e) => errors.push(e.code))
  assert.equal((await channel.subscribe()).ok, true)
  await new Promise((r) => setTimeout(r, 40))
  assert.equal(streams, 2)
  assert.deepEqual(errors, ['channel_not_authorized'])
  await assert.rejects(() => channel.send('typing', {}), /subscribe/)
  client.close()
})

// The server closes a stream whose token expired; the reconnect with the same
// token is refused, so the client stops. A refreshed token brings it back.
test('setAuth reconnects a stream that stopped on an expired token', async () => {
  const auths: string[] = []
  const statuses: string[] = []
  const ready = 'event: ready\ndata: {"stream_id":"n1.x","subscriptions":[{"sub":"s1","ok":true}]}\n\n'
  const client = createClient<Database>('https://example.test/sluice/v1', {
    accessToken: 'old',
    pauseWhenHidden: false,
    backoff: [1],
    onStatusChange: (s) => statuses.push(s),
    fetch: async (_input, init) => {
      const auth = new Headers(init?.headers).get('Authorization') ?? ''
      auths.push(auth)
      if (auths.length === 1) return sse(ready + 'event: error\ndata: {"code":"token_expired","message":"expired"}\n\n')
      if (auth === 'Bearer old') return new Response('{"error":"unauthorized"}', { status: 401 })
      return sse(openStream(ready))
    },
    onError: () => {},
  })
  await client.from('documents').as('s1').on('*', () => {}).subscribe()
  await new Promise((r) => setTimeout(r, 40))
  await client.setAuth('fresh')
  assert.deepEqual(auths, ['Bearer old', 'Bearer old', 'Bearer fresh'])
  // The server's close is a planned reconnect, and the refusal stops the
  // client: neither is reported as reconnecting.
  assert.deepEqual(statuses, ['connecting', 'open', 'connecting', 'closed', 'connecting', 'open'])
  client.close()
})

test('a failed attempt is reported as reconnecting', async () => {
  const statuses: string[] = []
  let calls = 0
  const client = createClient<Database>('https://example.test/sluice/v1', {
    accessToken: 'tok',
    pauseWhenHidden: false,
    backoff: [1],
    onStatusChange: (s) => statuses.push(s),
    onError: () => {},
    fetch: async () => {
      if (++calls === 1) throw new TypeError('network down')
      return sse(openStream('event: ready\ndata: {"stream_id":"n1.x","subscriptions":[{"sub":"s1","ok":true}]}\n\n'))
    },
  })
  await client.from('documents').as('s1').on('*', () => {}).subscribe()
  assert.deepEqual(statuses, ['connecting', 'reconnecting', 'open'])
  client.close()
})

test('a client with nothing to listen to is idle', async () => {
  const statuses: string[] = []
  const client = createClient<Database>('https://example.test/sluice/v1', {
    accessToken: 'tok',
    pauseWhenHidden: false,
    onStatusChange: (s) => statuses.push(s),
    fetch: async (input) => {
      if (String(input).endsWith('/unsubscribe')) return new Response(JSON.stringify({ removed: 1 }))
      return sse(openStream('event: ready\ndata: {"stream_id":"n1.x","subscriptions":[{"sub":"s1","ok":true}]}\n\n'))
    },
  })
  assert.equal(client.connectionStatus, 'idle')
  const sub = await client.from('documents').as('s1').on('*', () => {}).subscribe()
  await sub.unsubscribe()
  assert.deepEqual(statuses, ['connecting', 'open', 'idle'])
  client.close()
  assert.equal(client.connectionStatus, 'closed')
})

// ---------------------------------------------------------------------------
// Type-level checks. These fail at compile time, which is the point: the SDK's
// value is that a wrong column name or a wrong value type does not build.
// ---------------------------------------------------------------------------

test('type-level: schema, table, column and row narrowing', () => {
  const client = createClient<Database>('https://x/', { accessToken: 't', pauseWhenHidden: false })

  client
    .from('documents')
    .eq('owner_id', 'u1')
    .gte('id', 5)
    .is('archived', true)
    .select('id', 'title')
    .on('*', (c: ChangePayload<{ id: number; title: string }>) => {
      const id: number | undefined = c.record?.id
      const title: string | undefined = c.record?.title
      void id
      void title
      // @ts-expect-error `body` was not selected, so it is not on the row.
      void c.record?.body
    })

  // @ts-expect-error `nope` is not a table in this schema.
  client.from('nope')

  // @ts-expect-error `owner_id` is a string, not a number.
  client.from('documents').eq('owner_id', 42)

  // @ts-expect-error `not_a_column` does not exist on documents.
  client.from('documents').eq('not_a_column', 'x')

  client.schema('analytics').from('events').eq('kind', 'click')

  // @ts-expect-error `documents` is not in the analytics schema.
  client.schema('analytics').from('documents')

  assert.ok(true)
})
