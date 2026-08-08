/**
 * A minimal Server-Sent Events reader built on `fetch`, not `EventSource`.
 *
 * `EventSource` cannot set headers, cannot POST, and cannot change its
 * subscription set without reconnecting. Those three limitations are the only
 * reason realtime tokens have historically travelled in URLs. Reading the stream
 * with `fetch` instead lets the token live in an `Authorization` header where it
 * belongs, and lets the subscription set be sent as a request body.
 *
 * Only the *response* is streamed, which is universally supported. Request
 * streaming (`duplex: 'half'`) is still Chromium-only and is not used.
 */

export interface SSEEvent {
  event: string
  id?: string
  data: string
}

/**
 * Parses a `text/event-stream` body into events.
 *
 * Framing follows the WHATWG grammar: a blank line dispatches, `:` starts a
 * comment, and an event without its terminating blank line is discarded. That
 * last rule matters -- a truncated final event must not be delivered as if it
 * were complete.
 */
export async function* parseSSE(
  body: ReadableStream<Uint8Array>,
  signal?: AbortSignal,
): AsyncGenerator<SSEEvent> {
  const reader = body.getReader()
  const decoder = new TextDecoder()
  let buffer = ''
  let event = ''
  let id: string | undefined
  let data: string[] = []

  const onAbort = () => void reader.cancel().catch(() => {})
  signal?.addEventListener('abort', onAbort, { once: true })

  try {
    for (;;) {
      const { done, value } = await reader.read()
      if (done) break

      buffer += decoder.decode(value, { stream: true })

      // Normalise the three legal line terminators before splitting.
      let index: number
      while ((index = buffer.search(/\r\n|\r|\n/)) !== -1) {
        const line = buffer.slice(0, index)
        const width = buffer.startsWith('\r\n', index) ? 2 : 1
        buffer = buffer.slice(index + width)

        if (line === '') {
          if (event || data.length) {
            yield { event: event || 'message', id, data: data.join('\n') }
          }
          event = ''
          data = []
          continue
        }
        if (line.startsWith(':')) continue // comment, e.g. the heartbeat

        const colon = line.indexOf(':')
        const field = colon === -1 ? line : line.slice(0, colon)
        // A single leading space after the colon is part of the framing.
        let value_ = colon === -1 ? '' : line.slice(colon + 1)
        if (value_.startsWith(' ')) value_ = value_.slice(1)

        if (field === 'event') event = value_
        else if (field === 'data') data.push(value_)
        else if (field === 'id' && !value_.includes('\0')) id = value_
        // `retry` is ignored: reconnection is driven by the client's own backoff.
      }
    }
  } finally {
    signal?.removeEventListener('abort', onAbort)
    reader.releaseLock()
  }
}
