import { parseSSE } from './sse.js'
import {
  type AnyDatabase,
  type BroadcastPayload,
  type ChangePayload,
  type ClientOptions,
  type ConnectionStatus,
  type Json,
  type LiveEvent,
  type PresencePayload,
  type ReadyPayload,
  type ShapeSpec,
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
  /** Set once the server has installed it. */
  live?: boolean
  /**
   * Set while the server does not hold it and the client is to ask again
   * (`issuer_unavailable`): no resume covers that time.
   */
  lapsed?: boolean
}

interface PendingSubscribe {
  reg: Registration
  /** Asked for again by the client, with no caller waiting. */
  retry?: boolean
  resolve?(result: SubscriptionResult): void
  reject?(err: unknown): void
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
  /**
   * Resume position per relation, so a reconnect resumes instead of gapping:
   * the last commit LSN seen, or the position the server gave when the
   * relation's shapes went live.
   */
  private resumeFrom = new Map<string, string>()
  /** Subscriptions waiting to be asked for again, by label. */
  private retryTimers = new Map<string, ReturnType<typeof setTimeout>>()
  /** Results the current stream's ready event carried, by label. */
  private readyResults = new Map<string, SubscriptionResult>()

  private streamId: string | null = null
  private abort: AbortController | null = null
  private status: ConnectionStatus = 'idle'
  private attempt = 0
  private closed = false
  /** True while the stream loop runs; there is never more than one. */
  private running = false
  /** Set while the document is hidden: the loop stops and does not reconnect. */
  private paused = false
  /** Callers waiting for the next ready event, or for the loop to stop. */
  private waiters: Array<() => void> = []
  /** Token passed to setAuth, used instead of options.accessToken from then on. */
  private tokenOverride: string | null = null
  /** Subscriptions made in the same tick, sent together in one /subscribe. */
  private pending: PendingSubscribe[] = []
  /** The delay the server asked for before the next reconnect. */
  private retryAfter: number | null = null
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
        if (document.visibilityState === 'hidden') {
          this.paused = true
          this.disconnect()
        } else {
          this.paused = false
          if (!this.closed && this.registrations.size) void this.connect()
        }
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
      // If the stream request carried this subscription, its result arrived in
      // the ready event. If the request had already been sent when it was
      // registered, it is subscribed below like any later one.
      const carried = this.readyResults.get(reg.spec.sub)
      if (carried) return carried
      if (!this.streamId) {
        this.registrations.delete(reg.spec.sub)
        throw new SluiceError({ sub: reg.spec.sub, code: 'not_connected', message: 'the stream could not be opened', retryable: true })
      }
    }

    // Subscriptions made together -- a page mounting several live views -- go
    // out in one request, which the server resolves concurrently and counts
    // once against its subscribe rate.
    return new Promise<SubscriptionResult>((resolve, reject) => {
      if (this.pending.length === 0) queueMicrotask(() => void this.flushPending())
      this.pending.push({ reg, resolve, reject })
    })
  }

  private async flushPending(): Promise<void> {
    const batch = this.pending
    this.pending = []

    let body: Record<string, unknown>
    try {
      if (!this.streamId) {
        throw new SluiceError({ code: 'not_connected', message: 'the stream closed before the subscribe was sent', retryable: true })
      }
      body = await this.post('/subscribe', {
        stream_id: this.streamId,
        subscriptions: batch.map((p) => p.reg.spec),
      })
    } catch (err) {
      // A rejected subscribe leaves nothing registered, so retrying with the
      // same label works; a retry stays registered and is asked for again
      // later. The server may still have processed the request (a lost
      // response looks like a network error), so it is also asked to drop the
      // labels -- and that finishes before rejecting or retrying, so it cannot
      // race either.
      this.forget(batch.filter((p) => !p.retry).map((p) => p.reg.spec.sub))
      await this.dropOnServer(batch.map((p) => p.reg.spec.sub))
      for (const p of batch) {
        if (p.retry) this.scheduleRetry(p.reg.spec.sub)
        else p.reject?.(err)
      }
      return
    }
    const walLsn = body.wal_lsn as string | undefined
    const results = new Map((body.results as SubscriptionResult[] | undefined)?.map((r) => [r.sub, r]))
    for (const p of batch) {
      const result = results.get(p.reg.spec.sub) ?? {
        sub: p.reg.spec.sub,
        ok: false,
        error: { code: 'bad_response', message: 'the server returned no result for this subscription' },
      }
      this.handleResult(result, walLsn)
      p.resolve?.(result)
    }
  }

  /** @internal */
  async unregister(sub: string): Promise<void> {
    this.forget([sub])
    await this.dropOnServer([sub])
  }

  /** Forgets subscriptions on the client only. */
  private forget(subs: string[]): void {
    for (const sub of subs) {
      this.registrations.delete(sub)
      this.readyResults.delete(sub)
      clearTimeout(this.retryTimers.get(sub))
      this.retryTimers.delete(sub)
    }
  }

  /** Asks the server to drop subscriptions, and closes the stream when nothing is left. */
  private async dropOnServer(subs: string[]): Promise<void> {
    if (!this.streamId) return
    try {
      await this.post('/unsubscribe', { stream_id: this.streamId, subs })
    } catch {
      // A failed unsubscribe is harmless: the server drops everything when the
      // stream closes, and the stream is closed below if nothing is left.
    }
    if (this.registrations.size === 0) this.disconnect()
  }

  /**
   * Asks for a subscription again after a delay: the server could not decide
   * it yet (`issuer_unavailable`), or the request to ask again failed. A
   * reconnect resends every registration anyway, so without a stream there is
   * nothing to do.
   */
  private scheduleRetry(sub: string, delayMs = this.retryDelay()): void {
    clearTimeout(this.retryTimers.get(sub))
    this.retryTimers.set(
      sub,
      setTimeout(() => {
        this.retryTimers.delete(sub)
        const reg = this.registrations.get(sub)
        if (!reg || !this.streamId) return
        if (this.pending.length === 0) queueMicrotask(() => void this.flushPending())
        this.pending.push({ reg, retry: true })
      }, delayMs),
    )
  }

  /** Full jitter over the last backoff step, for a retry the server gave no delay for. */
  private retryDelay(): number {
    const schedule = this.options.backoff ?? DEFAULT_BACKOFF
    return Math.random() * (schedule[schedule.length - 1] ?? 1000)
  }

  private clearRetries(): void {
    for (const timer of this.retryTimers.values()) clearTimeout(timer)
    this.retryTimers.clear()
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
   * Uses a refreshed access token from now on, instead of `options.accessToken`.
   *
   * Call this when your auth library refreshes. On an open stream every
   * authorization decision is re-resolved server-side, because the claims may
   * have changed: a subscription that is no longer permitted is dropped with an
   * error rather than silently continuing. If the stream is down -- for example
   * after it closed with `token_expired` -- it reconnects with this token.
   */
  async setAuth(token: string): Promise<void> {
    this.tokenOverride = token
    if (this.streamId) {
      await this.post('/token', { stream_id: this.streamId, access_token: token })
    } else if (!this.closed && !this.paused && this.registrations.size > 0) {
      await this.connect()
    }
  }

  /** Closes the stream and forgets every subscription. */
  close(): void {
    this.closed = true
    this.registrations.clear()
    this.readyResults.clear()
    this.resumeFrom.clear()
    this.clearRetries()
    if (this.visibilityHandler && typeof document !== 'undefined') {
      document.removeEventListener('visibilitychange', this.visibilityHandler)
      this.visibilityHandler = null
    }
    this.disconnect()
  }

  // -------------------------------------------------------------------------
  // Transport
  // -------------------------------------------------------------------------

  private disconnect(): void {
    this.abort?.abort()
    this.abort = null
    this.streamId = null
    this.setStatus(this.closed ? 'closed' : 'idle')
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
    const source = this.options.accessToken
    const t = this.tokenOverride ?? (typeof source === 'function' ? await source() : source)
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
    let parsed: Record<string, unknown> = {}
    try {
      if (text) parsed = JSON.parse(text) as Record<string, unknown>
    } catch {
      // A proxy error page, not a Sluice response; reported by status below.
      if (res.ok) throw new SluiceError({ code: 'bad_response', message: 'the server did not return JSON', retryable: true })
    }
    if (!res.ok) {
      throw new SluiceError({
        code: (parsed.error as string) ?? `http_${res.status}`,
        message: (parsed.message as string) ?? res.statusText,
        retryable: res.status >= 500 || res.status === 429,
      })
    }
    return parsed
  }

  /**
   * Resolves on the next ready event, or when the stream loop stops. Starts the
   * loop if it is not running; there is never more than one.
   */
  private connect(): Promise<void> {
    const next = new Promise<void>((resolve) => this.waiters.push(resolve))
    if (!this.running) {
      this.running = true
      void this.runStream()
    }
    return next
  }

  private wake(): void {
    const waiters = this.waiters
    this.waiters = []
    for (const resolve of waiters) resolve()
  }

  /** Opens the stream and keeps it open, reconnecting with backoff. */
  private async runStream(): Promise<void> {
    // Only a failure is reported as reconnecting: a stream the server ended
    // cleanly, on token expiry or shutdown, is replaced as planned.
    let failed = false
    let refused = false
    try {
      while (!this.closed && !this.paused && this.registrations.size > 0) {
        this.setStatus(failed ? 'reconnecting' : 'connecting')
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
          failed = false
          for await (const ev of parseSSE(res.body, abort.signal)) {
            this.dispatch(ev.event, ev.data)
          }
          // A clean end of stream is still a disconnect: reconnect. A fresh
          // token is fetched, so a stream closed for token_expired recovers
          // when accessToken returns a new one.
        } catch (err) {
          if (abort.signal.aborted || this.closed) break
          const e = err instanceof SluiceError ? err : new SluiceError({
            code: 'connection_failed',
            message: err instanceof Error ? err.message : String(err),
            retryable: true,
          })
          this.options.onError?.(e)
          if (!e.retryable) {
            refused = true
            break
          }
          failed = true
        } finally {
          this.streamId = null
        }

        if (this.closed || this.paused || this.registrations.size === 0) break
        // Reported when the stream ends, not after the wait.
        this.setStatus(failed ? 'reconnecting' : 'connecting')

        const schedule = this.options.backoff ?? DEFAULT_BACKOFF
        const wait = schedule[Math.min(this.attempt, schedule.length - 1)] ?? 1000
        this.attempt++
        // Full jitter: a server restart must not bring every client back at the
        // same instant. A server that is shutting down spreads its clients
        // itself, over a wider window, and says when to come back.
        const delay = this.retryAfter ?? Math.random() * wait
        this.retryAfter = null
        await sleep(delay)
      }
    } finally {
      this.running = false
      this.streamId = null
      this.setStatus(this.closed || refused ? 'closed' : 'idle')
      this.wake()
    }
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
        this.readyResults.clear()
        // The stream request carried every registration, retries included.
        this.clearRetries()
        for (const result of ready.subscriptions ?? []) {
          this.readyResults.set(result.sub, result)
          this.handleResult(result, ready.wal_lsn)
        }
        this.wake()
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
          const reg = this.registrations.get(e.sub)
          // A subscription-scoped error that is not retryable ends that
          // subscription; the stream and every other subscription carry on.
          // It is forgotten before the handler runs, so the handler may
          // subscribe again under the same name. A retryable one with a delay
          // removed it for now: it is asked for again after the delay.
          if (!e.retryable) {
            this.forget([e.sub])
          } else if (reg && e.retry_after_ms !== undefined) {
            reg.lapsed = true
            this.scheduleRetry(e.sub, e.retry_after_ms)
          }
          reg?.onEvent('error', err)
        } else {
          // A stream-scoped error is followed by the server closing the
          // stream. The loop reconnects, and the server refuses the reconnect
          // (401) if the identity is no longer valid.
          if (typeof e.retry_after_ms === 'number' && e.retry_after_ms >= 0) this.retryAfter = e.retry_after_ms
          this.options.onError?.(err)
        }
        return
      }
    }
  }

  /** Applies a subscription result from a ready event or a /subscribe, with the position it came with. */
  private handleResult(result: SubscriptionResult, walLsn?: string): void {
    for (const w of result.warnings ?? []) this.options.onWarning?.(w)
    const reg = this.registrations.get(result.sub)
    // A refused subscription is not resent on every reconnect; one the server
    // could not decide yet is kept and asked for again.
    if (!result.ok) {
      if (!result.error?.retryable) {
        this.registrations.delete(result.sub)
      } else if (reg) {
        reg.lapsed = true
        this.scheduleRetry(result.sub, result.error.retry_after_ms)
      }
    }
    reg?.onResult(result)
    if (result.ok && reg?.spec.shape) this.live(reg, reg.spec.shape, result, walLsn)
  }

  /** Reports a shape the server installed, and keeps its table's resume position. */
  private live(reg: Registration, shape: ShapeSpec, result: SubscriptionResult, walLsn?: string): void {
    const table = `${shape.schema ?? 'public'}.${shape.table}`
    // A position the server could not resume from is stale, and a table with
    // none needs one; either way, resuming from walLsn later misses nothing. A
    // position being replayed is kept: the replayed changes advance it.
    if (walLsn && (result.resumed === false || !this.resumeFrom.has(table))) this.resumeFrom.set(table, walLsn)
    const reason = reg.live ? 'resubscribed' : 'subscribed'
    const live: LiveEvent = {
      sub: result.sub,
      reason,
      // Only a subscription that was installed until the stream ended is
      // covered by the replay.
      resumed: reason === 'resubscribed' && !reg.lapsed && result.resumed === true,
      walLsn,
    }
    reg.live = true
    reg.lapsed = false
    reg.onEvent('live', live)
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
