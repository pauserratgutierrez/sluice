/**
 * Wire protocol types and the type-level machinery that makes the client
 * typed against a generated database schema.
 *
 * The `Database` generic is deliberately structurally compatible with what
 * `supabase/postgres-meta` emits, so the same generated types file that
 * parameterises a PostgREST client also parameterises this one. The SDK is
 * hand-written because it has a public API and semver matters; the *types it
 * consumes* are generated, because a schema has no API to break.
 */

// ---------------------------------------------------------------------------
// Database schema shape (structurally compatible with generated types)
// ---------------------------------------------------------------------------

export type Json = string | number | boolean | null | { [key: string]: Json | undefined } | Json[]

/** The minimum structure the client needs from a generated types file. */
export type GenericTable = { Row: Record<string, unknown> }
export type GenericSchema = { Tables: Record<string, GenericTable> }

/**
 * The constraint every `DB` type parameter carries.
 *
 * A generated Supabase types file satisfies this structurally, so the same
 * `Database` type that parameterises a PostgREST client works here unchanged.
 */
export type GenericDatabase = { [schema: string]: GenericSchema }

/** No schema information: any table name and any column is accepted. */
export type AnyDatabase = { public: { Tables: { [table: string]: { Row: Record<string, unknown> } } } }

export type SchemaName<DB extends GenericDatabase> = Extract<keyof DB, string>

export type TableName<DB extends GenericDatabase, S extends keyof DB> = Extract<keyof DB[S]['Tables'], string>

export type RowOf<
  DB extends GenericDatabase,
  S extends keyof DB,
  T extends keyof DB[S]['Tables'],
> = DB[S]['Tables'][T]['Row']

export type ColumnOf<
  DB extends GenericDatabase,
  S extends keyof DB,
  T extends keyof DB[S]['Tables'],
> = Extract<keyof RowOf<DB, S, T>, string>

// ---------------------------------------------------------------------------
// Protocol
// ---------------------------------------------------------------------------

export type Operation = 'INSERT' | 'UPDATE' | 'DELETE' | 'TRUNCATE'

/** Which authorization strategy the server resolved a subscription to (RLS oracle). */
export type Tier = 'A' | 'B' | 'C'

/** Which shape oracle the server is running. Issuer mode has no tiers. */
export type Oracle = 'rls' | 'issuer'

export interface ShapeSpec {
  schema?: string
  table: string
  ops?: Operation[]
  filter?: string
  columns?: string[]
  /** `snapshot` asks the server for a consistent initial read before live changes. */
  initial?: 'none' | 'snapshot'
  /** Emit synthetic INSERT/DELETE when a row enters or leaves the shape. */
  transitions?: boolean
}

export interface SubscribeSpec {
  sub: string
  shape?: ShapeSpec
  channel?: string
  presence?: boolean
}

export interface SubscriptionWarning {
  code: string
  message: string
  effect?: string
  /** A runnable statement or concrete instruction. Always present in practice. */
  remedy?: string
}

export interface SluiceErrorPayload {
  sub?: string
  code: string
  message: string
  retryable?: boolean
  action?: string
  /**
   * How long to wait before trying again. Without `sub`, before reconnecting
   * (`server_shutdown`). With `sub`, the server removed or refused the
   * subscription (`issuer_unavailable`), and the client subscribes again after it.
   */
  retry_after_ms?: number
}

export interface SubscriptionResult {
  sub: string
  ok: boolean
  /** Present in issuer mode. Omitted or `"rls"` when the process uses GRANT+RLS. */
  oracle?: Oracle
  /** Authorization tier. Only the RLS oracle sends this; issuer mode omits it. */
  tier?: Tier
  /** Effective AND-only filter after issuer narrowing. */
  filter?: string
  indexed?: boolean
  routing_key?: string
  reason?: string
  error?: SluiceErrorPayload
  warnings?: SubscriptionWarning[]
  /**
   * Present when the request carried a resume position for the shape's table:
   * true when the server replays every change since it, false when it cannot.
   */
  resumed?: boolean
}

export interface ReadyPayload {
  stream_id: string
  server_time: string
  heartbeat_ms: number
  /** A resume position for these subscriptions: resuming from it replays whatever they miss. */
  wal_lsn?: string
  subscriptions: SubscriptionResult[]
}

/**
 * A shape subscription is live on the server: from now on, every change to the
 * shape reaches its handlers.
 *
 * It fires when the server has installed the subscription, before any change
 * committed afterwards can be missed, so a read made from this callback cannot
 * fall into a gap: every change it does not see is delivered. A change may
 * arrive before the callback; it is already part of the live stream.
 */
export interface LiveEvent {
  sub: string
  /** `subscribed` the first time the subscription goes live; `resubscribed` every time after. */
  reason: 'subscribed' | 'resubscribed'
  /**
   * True only when the server is replaying every change since the previous
   * stream, so nothing was missed. When false, changes may have been missed:
   * read the current state again.
   */
  resumed: boolean
  /** The resume position the server gave for this stream. */
  walLsn?: string
}

/** A row change, with the projection applied at the type level. */
export interface ChangePayload<Row = Record<string, unknown>> {
  sub: string
  op: Operation
  schema: string
  table: string
  commit_lsn: string
  commit_time?: string
  seq: number
  /** Absent for DELETE. */
  record?: Row
  /** Present when the table's replica identity carries it. */
  old?: Partial<Row>
  /**
   * Columns whose value the WAL did not carry because they hold an unchanged
   * TOASTed value. They are NOT null and NOT deleted -- merge them from your
   * existing copy of the row.
   */
  unchanged?: string[]
  /** `enter` or `leave` when an UPDATE moved the row across the shape boundary. */
  transition?: 'enter' | 'leave'
  /** Names a correctness caveat that applies to this event. Never silent. */
  degraded?: string
  /** True for rows delivered as part of an initial snapshot. */
  snapshot?: boolean
}

export interface BroadcastPayload<T = Json> {
  sub: string
  channel: string
  event: string
  payload: T
  from?: string
  origin?: 'client' | 'database'
  commit_lsn?: string
  at?: string
}

export interface PresenceMember<M = Json> {
  meta?: M
  since?: string
  ref?: string
}

export interface PresencePayload<M = Json> {
  sub: string
  channel: string
  type: 'state' | 'diff'
  members?: Record<string, PresenceMember<M>>
  joins?: Record<string, PresenceMember<M>>
  leaves?: Record<string, PresenceMember<M>>
}

export interface SnapshotEndPayload {
  sub: string
  rows: number
  floor_lsn: string
  /** More rows matched than the server's SLUICE_SNAPSHOT_MAX_ROWS; only that many were sent. */
  truncated?: boolean
}

/**
 * - `idle`: no stream, on purpose: nothing is subscribed, or the document is hidden.
 * - `connecting`: opening the stream, at first or to replace one the server ended (token expiry, shutdown).
 * - `open`: the stream is up.
 * - `reconnecting`: the stream dropped or an attempt to open it failed; retrying with backoff.
 * - `closed`: stopped and not retrying: `close()` was called, or the server refused the stream
 *   (401, 403). `setAuth` reopens a refused stream.
 */
export type ConnectionStatus = 'idle' | 'connecting' | 'open' | 'reconnecting' | 'closed'

export interface ClientOptions {
  /**
   * Returns the current access token, or null when signed out. Called on every
   * (re)connect and on refresh, so returning a live value is correct.
   */
  accessToken: string | (() => string | null | undefined | Promise<string | null | undefined>)
  /** Extra headers on every request. */
  headers?: Record<string, string>
  /** Custom fetch, for Node without global fetch or for instrumentation. */
  fetch?: typeof globalThis.fetch
  /** Reconnect backoff schedule in ms. The last value repeats. */
  backoff?: number[]
  /** Close the stream when the document is hidden, and reopen on return. */
  pauseWhenHidden?: boolean
  /** Called on connection state changes. */
  onStatusChange?: (status: ConnectionStatus) => void
  /** Called for stream-scoped errors that are not tied to one subscription. */
  onError?: (error: SluiceError) => void
  /** Called for server warnings. Wire this to your logger in production. */
  onWarning?: (warning: SubscriptionWarning) => void
  /** Emit protocol diagnostics to the console. */
  debug?: boolean
}

/** An error surfaced by the server or by the transport. */
export class SluiceError extends Error {
  readonly code: string
  readonly retryable: boolean
  readonly action?: string
  readonly sub?: string
  /** How long the server asked the client to wait before reconnecting. */
  readonly retryAfterMs?: number

  constructor(payload: SluiceErrorPayload) {
    super(payload.message)
    this.name = 'SluiceError'
    this.code = payload.code
    this.retryable = payload.retryable ?? false
    this.action = payload.action
    this.sub = payload.sub
    this.retryAfterMs = payload.retry_after_ms
  }
}
