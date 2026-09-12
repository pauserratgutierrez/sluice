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

test('builds a PostgREST-shaped filter string', async () => {
  let body: any
  const client = createClient<Database>('https://example.test/sluice/v1', {
    accessToken: 'tok',
    pauseWhenHidden: false,
    fetch: async (_input, init) => {
      body = JSON.parse(String(init?.body))
      return new Response('event: ready\ndata: {"stream_id":"n1.x","subscriptions":[]}\n\n', {
        status: 200,
        headers: { 'Content-Type': 'text/event-stream' },
      })
    },
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
    fetch: async () =>
      new Response(
        'event: ready\ndata: {"stream_id":"n1.x","subscriptions":[{"sub":"s1","ok":true}]}\n\n' +
          'event: change\ndata: ' + JSON.stringify(change) + '\n\n',
        { status: 200, headers: { 'Content-Type': 'text/event-stream' } },
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
    fetch: async () =>
      new Response('event: ready\ndata: {"stream_id":"n1.x","subscriptions":[]}\n\n', {
        status: 200,
        headers: { 'Content-Type': 'text/event-stream' },
      }),
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
