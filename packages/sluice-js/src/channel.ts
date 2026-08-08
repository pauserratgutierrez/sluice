import type { SluiceClient } from './client.js'
import type { GenericDatabase } from './types.js'
import type {
  BroadcastPayload,
  Json,
  PresenceMember,
  PresencePayload,
  SluiceError,
  SubscriptionResult,
} from './types.js'

let counter = 0
const nextLabel = () => `c${(++counter).toString(36)}`

export interface ChannelSubscription {
  ok: boolean
  error?: SluiceError
  unsubscribe(): Promise<void>
}

/**
 * A signalling channel: ephemeral broadcast plus presence.
 *
 * Nothing here touches the database. Broadcast is fire-and-forget with no
 * persistence; presence is keyed, last-write-wins membership held in server
 * memory, cleaned up automatically when the stream closes.
 *
 * Unlike some realtime clients, `send` is a request with a response: an
 * oversized payload or an unauthorized channel throws, rather than being
 * silently dropped.
 */
export class Channel<M = Json> {
  private readonly label = nextLabel()
  private handlers = new Map<string, Array<(payload: never, meta: BroadcastPayload) => void>>()
  private presenceState: Record<string, PresenceMember<M>> = {}
  private presenceHandlers: Array<(state: Record<string, PresenceMember<M>>) => void> = []
  private joinHandlers: Array<(joins: Record<string, PresenceMember<M>>) => void> = []
  private leaveHandlers: Array<(leaves: Record<string, PresenceMember<M>>) => void> = []
  private errorHandler?: (e: SluiceError) => void
  private wantPresence = false
  private subscribed = false

  constructor(
    private readonly client: SluiceClient<GenericDatabase>,
    readonly name: string,
  ) {}

  /** Registers a handler for a broadcast event name. */
  on<T = Json>(event: string, handler: (payload: T, meta: BroadcastPayload<T>) => void): this {
    const list = this.handlers.get(event) ?? []
    list.push(handler as unknown as (p: never, m: BroadcastPayload) => void)
    this.handlers.set(event, list)
    return this
  }

  onError(handler: (error: SluiceError) => void): this {
    this.errorHandler = handler
    return this
  }

  /** Enables presence and registers a full-state handler. */
  onPresence(handler: (state: Record<string, PresenceMember<M>>) => void): this {
    this.wantPresence = true
    this.presenceHandlers.push(handler)
    return this
  }

  onJoin(handler: (joins: Record<string, PresenceMember<M>>) => void): this {
    this.wantPresence = true
    this.joinHandlers.push(handler)
    return this
  }

  onLeave(handler: (leaves: Record<string, PresenceMember<M>>) => void): this {
    this.wantPresence = true
    this.leaveHandlers.push(handler)
    return this
  }

  /** The current presence roster, maintained locally from state and diffs. */
  get presence(): Readonly<Record<string, PresenceMember<M>>> {
    return this.presenceState
  }

  /** Joins the channel. */
  async subscribe(): Promise<ChannelSubscription> {
    let resolved: SubscriptionResult | undefined
    const result = await this.client.register({
      spec: { sub: this.label, channel: this.name, presence: this.wantPresence },
      onResult: (r) => {
        resolved = r
      },
      onEvent: (kind, payload) => {
        if (kind === 'broadcast') {
          const b = payload as BroadcastPayload
          for (const h of this.handlers.get(b.event) ?? []) h(b.payload as never, b)
          for (const h of this.handlers.get('*') ?? []) h(b.payload as never, b)
        } else if (kind === 'presence') {
          this.applyPresence(payload as PresencePayload<M>)
        } else if (kind === 'error') {
          this.errorHandler?.(payload as SluiceError)
        }
      },
    })

    const final = resolved ?? result
    this.subscribed = final.ok
    return {
      ok: final.ok,
      error: final.error as unknown as SluiceError | undefined,
      unsubscribe: async () => {
        this.subscribed = false
        await this.client.unregister(this.label)
      },
    }
  }

  /**
   * Broadcasts to everyone else on the channel.
   *
   * @param self include the sender
   * @returns how many streams the message reached
   */
  async send(event: string, payload: unknown, self = false): Promise<number> {
    if (!this.subscribed) throw new Error(`sluice: subscribe to "${this.name}" before sending to it`)
    return this.client.publish(this.name, event, payload, self)
  }

  /** Announces this client's presence, or updates its metadata. */
  async track(meta?: M): Promise<void> {
    await this.client.presence(this.name, 'track', meta)
  }

  /** Withdraws presence. Also happens automatically when the stream closes. */
  async untrack(): Promise<void> {
    await this.client.presence(this.name, 'untrack')
  }

  private applyPresence(p: PresencePayload<M>): void {
    if (p.type === 'state') {
      this.presenceState = { ...(p.members ?? {}) }
      for (const h of this.presenceHandlers) h(this.presenceState)
      return
    }
    if (p.joins) {
      for (const [k, v] of Object.entries(p.joins)) this.presenceState[k] = v
      for (const h of this.joinHandlers) h(p.joins)
    }
    if (p.leaves) {
      for (const k of Object.keys(p.leaves)) delete this.presenceState[k]
      for (const h of this.leaveHandlers) h(p.leaves)
    }
    for (const h of this.presenceHandlers) h(this.presenceState)
  }
}
