import { parseSSE } from './sse.js'
import {
  type AnyDatabase,
  type BroadcastPayload,
  type ChangePayload,
  type ClientOptions,
  type ConnectionStatus,
  type Json,
  type PresencePayload,
  type ReadyPayload,
  type SnapshotEndPayload,
  type SubscribeSpec,
  type SubscriptionResult,
  type SubscriptionWarning,
  SluiceError,
  type SluiceErrorPayload,
  type TableName,
  type SchemaName,
  type GenericDatabase,
  type RowOf,
} from './types.js'
import { ShapeBuilder, type ShapeSubscription } from './shape.js'
import { Channel } from './channel.js'

const DEFAULT_BACKOFF = [500, 1000, 2000, 5000, 10_000]

interface Registration {
  spec: SubscribeSpec
  onEvent(kind: string, payload: unknown): void
  onResult(result: SubscriptionResult): void
}

/**
 * A Sluice connection.
 *
 * One client holds ONE stream and multiplexes every subscription over it. Over
 * HTTP/2 that stream and the control POSTs share a single connection, so this is
 * one socket regardless of how many tables and channels you watch.
 */
export class SluiceClient<DB extends GenericDatabase = AnyDatabase> {
  readonly url: string
  private readonly options: ClientOptions
  private readonly fetchImpl: typeof globalThis.fetch

  private registrations = new Map<string, Registration>()
  /** Last commit LSN seen per relation, so a reconnect resumes instead of gapping. */
  private resumeFrom = new Map<string, string>()

  private streamId: string | null = null
  private abort: AbortController | null = null
  private status: ConnectionStatus = 'closed'
  private attempt = 0
  private closed = false
  private connectPromise: Promise<void> | null = null
  private visibilityHandler: (() => void) | null = null

  constructor(url: string, options: ClientOptions) {
    this.url = url.replace(/\/+$/, '')
    this.options = options
    this.fetchImpl = options.fetch ?? globalThis.fetch.bind(globalThis)

    if (options.pauseWhenHidden !== false && typeof document !== 'undefined') {
      // Closing a hidden stream is the standard mitigation for the tension
      // between proxies wanting frequent keepalives and mobile radios wanting
      // silence. It also stops background tabs holding server resources.
      this.visibilityHandler = () => {
        if (document.visibilityState === 'hidden') this.disconnect()
        else if (!this.closed && this.registrations.size) void this.connect()
      }
      document.addEventListener('visibilitychange', this.visibilityHandler)
    }
  }

  /** Current connection status. */
  get connectionStatus(): ConnectionStatus {
    return this.status
  }

  /**
   * Starts building a typed subscription to a table.
   *
   * ```ts
   * client.from('documents').eq('owner_id', userId).select('id', 'title')
   *   .on('*', c => console.log(c.record.title))
   *   .subscribe()
   * ```
   */
  from<T extends TableName<DB, 'public'>>(
    table: T,
  ): ShapeBuilder<DB, 'public', T, RowOf<DB, 'public', T>> {
    return new ShapeBuilder(this as SluiceClient<GenericDatabase>, 'public', table)
  }

  /** Same as `from`, for a schema other than `public`. */
  schema<S extends SchemaName<DB>>(schema: S) {
    return {
      from: <T extends TableName<DB, S>>(table: T): ShapeBuilder<DB, S, T, RowOf<DB, S, T>> =>
        new ShapeBuilder(this as SluiceClient<GenericDatabase>, schema, table),
    }
  }

  /** Opens a signalling channel for broadcast and presence. */
  channel<M = Json>(name: string): Channel<M> {
    return new Channel<M>(this as SluiceClient<GenericDatabase>, name)
  }

  /**
   * Registers a subscription and ensures the stream is running.
   *
   * @internal Used by ShapeBuilder and Channel.
   */
  async register(reg: Registration): Promise<SubscriptionResult> {
    if (this.registrations.has(reg.spec.sub)) {
      throw new Error(`sluice: a subscription named "${reg.spec.sub}" already exists on this client`)
    }
    this.registrations.set(reg.spec.sub, reg)
    this.closed = false

    if (!this.streamId) {
      await this.connect()
      // The stream request carried this subscription, so its result already
      // arrived in the ready event.
      return this.lastResults.get(reg.spec.sub) ?? { sub: reg.spec.sub, ok: true }
    }

    const body = await this.post('/subscribe', {
      stream_id: this.streamId,
      subscriptions: [reg.spec],
    })
    const result = (body.results as SubscriptionResult[])?.[0] ?? { sub: reg.spec.sub, ok: true }
    this.handleResult(result)
    return result
  }

  /** @internal */
  async unregister(sub: string): Promise<void> {
    this.registrations.delete(sub)
    if (!this.streamId) return
    try {
      await this.post('/unsubscribe', { stream_id: this.streamId, subs: [sub] })
    } catch {
      // A failed unsubscribe is harmless: the server drops everything when the
      // stream closes, and the stream is closed below if nothing is left.
    }
    if (this.registrations.size === 0) this.disconnect()
  }

  /** @internal */
  async publish(channel: string, event: string, payload: unknown, self = false): Promise<number> {
    const body = await this.post('/publish', {
      stream_id: await this.requireStream(),
      channel,
      event,
      payload,
      self,
    })
    return (body.delivered as number) ?? 0
  }

  /** @internal */
  async presence(channel: string, action: 'track' | 'update' | 'untrack', meta?: unknown, key?: string) {
    await this.post('/presence', {
      stream_id: await this.requireStream(),
      channel,
      action,
      key,
      meta,
    })
  }

  /**
   * Rebinds the stream to a refreshed access token.
   *
   * Call this when your auth library refreshes. Every authorization decision on
   * the stream is re-resolved server-side, because the claims may have changed:
   * a subscription that is no longer permitted is dropped with an error rather
   * than silently continuing.
   */
  async setAuth(token: string): Promise<void> {
    if (!this.streamId) return
    await this.post('/token', { stream_id: this.streamId, access_token: token })
  }

  /** Closes the stream and forgets every subscription. */
  close(): void {
    this.closed = true
    this.registrations.clear()
    this.resumeFrom.clear()
    if (this.visibilityHandler && typeof document !== 'undefined') {
      document.removeEventListener('visibilitychange', this.visibilityHandler)
      this.visibilityHandler = null
    }
    this.disconnect()
  }

  // -------------------------------------------------------------------------
  // Transport
  // -------------------------------------------------------------------------

  private lastResults = new Map<string, SubscriptionResult>()

  private disconnect(): void {
    this.abort?.abort()
    this.abort = null
    this.streamId = null
    this.setStatus(this.closed ? 'closed' : 'reconnecting')
  }

  private setStatus(status: ConnectionStatus): void {
    if (this.status === status) return
    this.status = status
    this.options.onStatusChange?.(status)
  }

  private async requireStream(): Promise<string> {
    if (!this.streamId) await this.connect()
    if (!this.streamId) throw new Error('sluice: not connected')
    return this.streamId
  }

  private async token(): Promise<string> {
    const t = typeof this.options.accessToken === 'function' ? await this.options.accessToken() : this.options.accessToken
    if (!t) throw new SluiceError({ code: 'no_token', message: 'no access token available', retryable: true })
    return t
  }

  private async headers(): Promise<Record<string, string>> {
    return {
      ...this.options.headers,
      Authorization: `Bearer ${await this.token()}`,
      'Content-Type': 'application/json',
    }
  }

  private async post(path: string, body: unknown): Promise<Record<string, unknown>> {
    const headers = await this.headers()
    if (this.streamId) headers['Sluice-Stream-Id'] = this.streamId

    const res = await this.fetchImpl(this.url + path, {
      method: 'POST',
      headers,
      body: JSON.stringify(body),
    })
    const text = await res.text()
    const parsed = text ? (JSON.parse(text) as Record<string, unknown>) : {}
    if (!res.ok) {
      throw new SluiceError({
        code: (parsed.error as string) ?? `http_${res.status}`,
        message: (parsed.message as string) ?? res.statusText,
        retryable: res.status >= 500 || res.status === 429,
      })
    }
    return parsed
  }

  /** Opens the stream, retrying with backoff until closed. */
  private connect(): Promise<void> {
    if (this.connectPromise) return this.connectPromise
    this.connectPromise = this.connectLoop().finally(() => {
      this.connectPromise = null
    })
    return this.connectPromise
  }

  private async connectLoop(): Promise<void> {
    let resolve!: () => void
    const opened = new Promise<void>((r) => {
      resolve = r
    })
    void this.runStream(resolve)
    return opened
  }

  private async runStream(opened: () => void): Promise<void> {
    let settled = false
    const settle = () => {
      if (!settled) {
        settled = true
        opened()
      }
    }

    while (!this.closed && this.registrations.size > 0) {
      this.setStatus(this.attempt === 0 ? 'connecting' : 'reconnecting')
      const abort = new AbortController()
      this.abort = abort

      try {
        const resume: Record<string, string> = {}
        for (const [rel, lsn] of this.resumeFrom) resume[rel] = lsn

        const res = await this.fetchImpl(this.url + '/stream', {
          method: 'POST',
          headers: { ...(await this.headers()), Accept: 'text/event-stream' },
          body: JSON.stringify({
            subscriptions: [...this.registrations.values()].map((r) => r.spec),
            resume,
          }),
          signal: abort.signal,
        })

        if (!res.ok || !res.body) {
          const text = await res.text().catch(() => '')
          throw new SluiceError({
            code: `http_${res.status}`,
            message: text || res.statusText,
            // 401 means the token is wrong, not that the server is busy.
            retryable: res.status !== 401 && res.status !== 403,
          })
        }

        this.attempt = 0
        for await (const ev of parseSSE(res.body, abort.signal)) {
          this.dispatch(ev.event, ev.data)
          if (ev.event === 'ready') settle()
        }
        // A clean end of stream is still a disconnect: reconnect.
      } catch (err) {
        if (abort.signal.aborted || this.closed) break
        const e = err instanceof SluiceError ? err : new SluiceError({
          code: 'connection_failed',
          message: err instanceof Error ? err.message : String(err),
          retryable: true,
        })
        this.options.onError?.(e)
        if (!e.retryable) {
          settle()
          break
        }
      }

      if (this.closed || this.registrations.size === 0) break

      const schedule = this.options.backoff ?? DEFAULT_BACKOFF
      const wait = schedule[Math.min(this.attempt, schedule.length - 1)] ?? 1000
      this.attempt++
      // Full jitter: a server restart must not bring every client back at the
      // same instant.
      await sleep(Math.random() * wait)
    }

    settle()
    this.streamId = null
    this.setStatus(this.closed ? 'closed' : 'reconnecting')
  }

  private dispatch(kind: string, raw: string): void {
    let payload: unknown
    try {
      payload = JSON.parse(raw)
    } catch {
      return
    }
    if (this.options.debug) console.debug('[sluice]', kind, payload)

    switch (kind) {
      case 'ready': {
        const ready = payload as ReadyPayload
        this.streamId = ready.stream_id
        this.setStatus('open')
        for (const result of ready.subscriptions ?? []) this.handleResult(result)
        return
      }
      case 'change': {
        const change = payload as ChangePayload
        // Remember the position so a reconnect resumes rather than gapping.
        this.resumeFrom.set(`${change.schema}.${change.table}`, change.commit_lsn)
        this.registrations.get(change.sub)?.onEvent('change', change)
        return
      }
      case 'broadcast': {
        const b = payload as BroadcastPayload
        this.registrations.get(b.sub)?.onEvent('broadcast', b)
        return
      }
      case 'presence': {
        const p = payload as PresencePayload
        this.registrations.get(p.sub)?.onEvent('presence', p)
        return
      }
      case 'snapshot_end': {
        const s = payload as SnapshotEndPayload
        this.registrations.get(s.sub)?.onEvent('snapshot_end', s)
        return
      }
      case 'warning': {
        const w = payload as SubscriptionWarning & { sub?: string }
        this.options.onWarning?.(w)
        if (w.sub) this.registrations.get(w.sub)?.onEvent('warning', w)
        return
      }
      case 'error': {
        const e = payload as SluiceErrorPayload
        const err = new SluiceError(e)
        if (e.sub) {
          this.registrations.get(e.sub)?.onEvent('error', err)
          // A subscription-scoped error is terminal for that subscription; the
          // stream and every other subscription carry on.
          if (!e.retryable) this.registrations.delete(e.sub)
        } else {
          this.options.onError?.(err)
          // A stream-scoped error means the server is about to close us.
          if (e.code === 'session_revoked' || e.code === 'token_expired') {
            this.disconnect()
          }
        }
        return
      }
    }
  }

  private handleResult(result: SubscriptionResult): void {
    this.lastResults.set(result.sub, result)
    for (const w of result.warnings ?? []) this.options.onWarning?.(w)
    this.registrations.get(result.sub)?.onResult(result)
  }
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms))
}

/**
 * Creates a Sluice client.
 *
 * @param url  the Sluice base URL, e.g. `https://api.example.com/sluice/v1`
 */
export function createClient<DB extends GenericDatabase = AnyDatabase>(
  url: string,
  options: ClientOptions,
): SluiceClient<DB> {
  return new SluiceClient<DB>(url, options)
}

export type { ShapeSubscription }
