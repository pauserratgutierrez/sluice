import type { SluiceClient } from './client.js'
import {
  SluiceError,
  type ChangePayload,
  type GenericDatabase,
  type Operation,
  type RowOf,
  type ShapeSpec,
  type SubscriptionResult,
  type SubscriptionWarning,
  type Oracle,
  type Tier,
} from './types.js'

/** Values a filter may compare against. */
type FilterValue = string | number | boolean | null

const encode = (value: FilterValue): string => (value === null ? 'null' : String(value))

let counter = 0
const nextLabel = () => `s${(++counter).toString(36)}`

/**
 * A fluent, typed builder for a table subscription.
 *
 * Filters are AND-only by design. `OR` is not missing by oversight: it destroys
 * the constant indexing that lets the server route a change with a map lookup
 * instead of a scan, which is the difference between throughput that is flat in
 * subscriber count and throughput that degrades linearly with it. Register two
 * subscriptions instead.
 *
 * Prefer at least one `.eq()` on an indexed column. Under the RLS oracle that
 * is also what lets the server reduce the policy to a constant; the `tier` on
 * the returned subscription tells you whether it managed to. Under the issuer
 * oracle there is no tier — look at `oracle` and the effective `filter`.
 *
 * `Row` narrows as you call `.select()`, so handlers see exactly the columns you
 * asked for.
 */
export class ShapeBuilder<
  DB extends GenericDatabase,
  S extends keyof DB & string,
  T extends keyof DB[S]['Tables'] & string,
  Row = RowOf<DB, S, T>,
> {
  private filters: string[] = []
  private cols?: readonly string[]
  private opsList?: Operation[]
  private initialMode: 'none' | 'snapshot' = 'none'
  private wantTransitions = false
  private label = nextLabel()

  private handlers: Array<{ op: Operation | '*'; fn: (c: ChangePayload<Row>) => void }> = []
  private errorHandler?: (e: SluiceError) => void
  private warningHandler?: (w: SubscriptionWarning) => void
  private snapshotEndHandler?: (info: { rows: number }) => void

  constructor(
    private readonly client: SluiceClient<GenericDatabase>,
    private readonly schemaName: string,
    private readonly tableName: T,
  ) {}

  /** Names the subscription, so warnings and errors are identifiable. */
  as(name: string): this {
    this.label = name
    return this
  }

  private add<C extends keyof Row & string>(column: C, op: string, rendered: string, negate = false): this {
    this.filters.push(`${column}=${negate ? 'not.' : ''}${op}.${rendered}`)
    return this
  }

  eq<C extends keyof Row & string>(column: C, value: Row[C] & FilterValue): this {
    return this.add(column, 'eq', encode(value))
  }
  neq<C extends keyof Row & string>(column: C, value: Row[C] & FilterValue): this {
    return this.add(column, 'neq', encode(value))
  }
  gt<C extends keyof Row & string>(column: C, value: Row[C] & FilterValue): this {
    return this.add(column, 'gt', encode(value))
  }
  gte<C extends keyof Row & string>(column: C, value: Row[C] & FilterValue): this {
    return this.add(column, 'gte', encode(value))
  }
  lt<C extends keyof Row & string>(column: C, value: Row[C] & FilterValue): this {
    return this.add(column, 'lt', encode(value))
  }
  lte<C extends keyof Row & string>(column: C, value: Row[C] & FilterValue): this {
    return this.add(column, 'lte', encode(value))
  }
  /** At most 100 values; beyond that the server refuses the shape. */
  in<C extends keyof Row & string>(column: C, values: readonly (Row[C] & FilterValue)[]): this {
    return this.add(column, 'in', `(${values.map(encode).join(',')})`)
  }
  /** `*` is the wildcard, as in PostgREST. */
  like<C extends keyof Row & string>(column: C, pattern: string): this {
    return this.add(column, 'like', pattern)
  }
  ilike<C extends keyof Row & string>(column: C, pattern: string): this {
    return this.add(column, 'ilike', pattern)
  }
  is<C extends keyof Row & string>(column: C, value: null | boolean): this {
    return this.add(column, 'is', value === null ? 'null' : String(value))
  }
  /** Negated comparison, e.g. `.notEq('status', 'draft')`. */
  notEq<C extends keyof Row & string>(column: C, value: Row[C] & FilterValue): this {
    return this.add(column, 'eq', encode(value), true)
  }
  notIn<C extends keyof Row & string>(column: C, values: readonly (Row[C] & FilterValue)[]): this {
    return this.add(column, 'in', `(${values.map(encode).join(',')})`, true)
  }
  notLike<C extends keyof Row & string>(column: C, pattern: string): this {
    return this.add(column, 'like', pattern, true)
  }

  /**
   * Restricts the projection, narrowing the type handlers receive.
   *
   * The replica identity columns are always included regardless, because without
   * them a client cannot identify the row at all.
   */
  select<C extends readonly (keyof Row & string)[]>(
    ...columns: C
  ): ShapeBuilder<DB, S, T, Pick<Row, C[number]>> {
    this.cols = columns
    return this as unknown as ShapeBuilder<DB, S, T, Pick<Row, C[number]>>
  }

  /** Restricts which operations are delivered. Defaults to INSERT, UPDATE and DELETE. */
  ops(...ops: Operation[]): this {
    this.opsList = ops
    return this
  }

  /**
   * Asks the server for a consistent initial read before live changes.
   *
   * This closes the race you would otherwise have between fetching initial state
   * over HTTP and subscribing: the server takes its replay floor before the
   * snapshot transaction begins, so nothing can slip through the gap. Rows arrive
   * as `INSERT` events with `snapshot: true`, terminated by `onSnapshotEnd`.
   */
  withInitialSnapshot(): this {
    this.initialMode = 'snapshot'
    return this
  }

  /**
   * Emits a synthetic `INSERT` when an UPDATE moves a row into the shape and a
   * synthetic `DELETE` when it moves out.
   *
   * Without this, a row that stops matching your filter simply stops producing
   * events, and your local copy silently keeps a row that no longer belongs.
   */
  withTransitions(): this {
    this.wantTransitions = true
    return this
  }

  /** Registers a change handler. Use `'*'` for every operation. */
  on(op: Operation | '*', handler: (change: ChangePayload<Row>) => void): this {
    this.handlers.push({ op, fn: handler })
    return this
  }

  onError(handler: (error: SluiceError) => void): this {
    this.errorHandler = handler
    return this
  }

  onWarning(handler: (warning: SubscriptionWarning) => void): this {
    this.warningHandler = handler
    return this
  }

  onSnapshotEnd(handler: (info: { rows: number }) => void): this {
    this.snapshotEndHandler = handler
    return this
  }

  /** Registers the subscription with the server and starts the stream. */
  async subscribe(): Promise<ShapeSubscription> {
    const spec: ShapeSpec = {
      schema: this.schemaName,
      table: this.tableName,
      ...(this.filters.length ? { filter: this.filters.join(',') } : {}),
      ...(this.cols ? { columns: [...this.cols] } : {}),
      ...(this.opsList ? { ops: this.opsList } : {}),
      ...(this.initialMode === 'snapshot' ? { initial: 'snapshot' as const } : {}),
      ...(this.wantTransitions ? { transitions: true } : {}),
    }

    let resolved: SubscriptionResult | undefined
    const result = await this.client.register({
      spec: { sub: this.label, shape: spec },
      onResult: (r) => {
        resolved = r
        for (const w of r.warnings ?? []) this.warningHandler?.(w)
        if (!r.ok && r.error) this.errorHandler?.(new SluiceError(r.error))
      },
      onEvent: (kind, payload) => {
        switch (kind) {
          case 'change': {
            const c = payload as ChangePayload<Row>
            for (const h of this.handlers) if (h.op === '*' || h.op === c.op) h.fn(c)
            return
          }
          case 'error':
            this.errorHandler?.(payload as SluiceError)
            return
          case 'warning':
            this.warningHandler?.(payload as SubscriptionWarning)
            return
          case 'snapshot_end':
            this.snapshotEndHandler?.(payload as { rows: number })
            return
        }
      },
    })

    const final = resolved ?? result
    return {
      sub: this.label,
      ok: final.ok,
      oracle: final.oracle,
      tier: final.tier,
      filter: final.filter,
      indexed: final.indexed,
      routingKey: final.routing_key,
      reason: final.reason,
      warnings: final.warnings ?? [],
      error: final.error ? new SluiceError(final.error) : undefined,
      unsubscribe: () => this.client.unregister(this.label),
    }
  }
}

/** A live table subscription. */
export interface ShapeSubscription {
  sub: string
  ok: boolean
  /** `"issuer"` when the server uses the HTTP shape issuer; otherwise omitted or `"rls"`. */
  oracle?: Oracle
  /**
   * Which authorization strategy the RLS oracle resolved to.
   *
   * Only present when the server runs `SLUICE_SHAPE_ORACLE=rls`. Do not assume
   * `"A"` in issuer mode. `A` costs nothing per change. `B` is evaluated in
   * process, also free of database work. `C` runs one impersonated query per
   * change PER SUBSCRIBER — `reason` and `warnings` say why, and `/diagnostics`
   * suggests a rewrite.
   */
  tier?: Tier
  /** Effective filter after issuer narrowing, when the server sent one. */
  filter?: string
  /** False means the shape is scanned for every change to the table. */
  indexed?: boolean
  routingKey?: string
  reason?: string
  warnings: SubscriptionWarning[]
  error?: SluiceError
  unsubscribe(): Promise<void>
}
