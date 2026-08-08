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

/** Which authorization strategy the server resolved a subscription to. */
export type Tier = 'A' | 'B' | 'C'

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
}

export interface SubscriptionResult {
  sub: string
  ok: boolean
  tier?: Tier
  indexed?: boolean
  routing_key?: string
  reason?: string
  error?: SluiceErrorPayload
  warnings?: SubscriptionWarning[]
}

export interface ReadyPayload {
  stream_id: string
  server_time: string
  heartbeat_ms: number
  wal_lsn?: string
  subscriptions: SubscriptionResult[]
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
}

export type ConnectionStatus = 'connecting' | 'open' | 'reconnecting' | 'closed'

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

  constructor(payload: SluiceErrorPayload) {
    super(payload.message)
    this.name = 'SluiceError'
    this.code = payload.code
    this.retryable = payload.retryable ?? false
    this.action = payload.action
    this.sub = payload.sub
  }
}
