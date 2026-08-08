/**
 * Sluice client.
 *
 * A realtime client for PostgreSQL that streams row changes, ephemeral broadcast
 * and presence over a single SSE connection, with the access token in an
 * `Authorization` header rather than a query parameter.
 *
 * @example
 * ```ts
 * import { createClient } from '@pauserratgutierrez/sluice-js'
 * import type { Database } from '@gatherpeers/database-types'
 *
 * const sluice = createClient<Database>('https://api.example.com/sluice/v1', {
 *   accessToken: () => supabase.auth.getSession().then(s => s.data.session?.access_token),
 * })
 *
 * const docs = await sluice
 *   .from('documents')
 *   .eq('owner_id', userId)
 *   .select('id', 'title', 'updated_at')
 *   .withInitialSnapshot()
 *   .on('*', ({ op, record }) => console.log(op, record?.title))
 *   .subscribe()
 *
 * if (docs.tier === 'C') {
 *   console.warn('this subscription authorizes per change', docs.warnings)
 * }
 * ```
 */

export { createClient, SluiceClient } from './client.js'
export { ShapeBuilder, type ShapeSubscription } from './shape.js'
export { Channel, type ChannelSubscription } from './channel.js'
export { SluiceError } from './types.js'

// Exported so a custom transport can reuse the framing, and so the test suite
// can exercise it against the built output.
export { parseSSE, type SSEEvent } from './sse.js'

export type {
  AnyDatabase,
  BroadcastPayload,
  ChangePayload,
  ClientOptions,
  ColumnOf,
  ConnectionStatus,
  GenericDatabase,
  GenericSchema,
  GenericTable,
  Json,
  Operation,
  PresenceMember,
  PresencePayload,
  ReadyPayload,
  RowOf,
  ShapeSpec,
  SnapshotEndPayload,
  SubscribeSpec,
  SubscriptionResult,
  SubscriptionWarning,
  TableName,
  Tier,
} from './types.js'
