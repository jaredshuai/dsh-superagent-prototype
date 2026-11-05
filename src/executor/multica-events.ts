/**
 * The Multica workspace event stream over WebSocket: authenticates with the first frame, hands each event to a listener,
 * and reconnects with backoff. Events missed while disconnected are not replayed by the server, so every (re)connection
 * calls `connected`, where the listener catches up through REST. This is the one place that knows the WebSocket protocol,
 * which Multica does not promise as a public API.
 */
import type { MulticaConnection } from './multica-api.ts'

/** One workspace event, such as `task:completed` or `comment:created`. */
export interface MulticaEvent { type: string; event_id?: string; actor_type?: string; actor_id?: string; payload: Record<string, unknown> }

export interface MulticaEventsListener {
  /** The stream is (again) connected and authenticated: anything that happened before may have been missed. */
  connected(): void
  event(event: MulticaEvent): void
  /** The stream dropped or could not authenticate, with why; it reconnects on its own. */
  disconnected(reason: string): void
}

/** The part of a WebSocket the stream uses, so tests can pass their own. */
export interface SocketLike {
  readyState: number
  send(data: string): void
  close(): void
  onopen: ((event: unknown) => void) | null
  onmessage: ((event: { data: unknown }) => void) | null
  onclose: ((event: { code?: number; reason?: string }) => void) | null
  onerror: ((event: unknown) => void) | null
}
export type SocketFactory = (url: string) => SocketLike

const BACKOFF_MS = [1000, 2000, 5000, 15_000, 30_000, 60_000] as const
/** How long the server may take to acknowledge the auth frame before the connection counts as failed. */
const AUTH_TIMEOUT_MS = 15_000
/** Application-level ping interval; the server also pings at the protocol level. */
const PING_MS = 30_000

/**
 * Keeps one authenticated connection to a Multica workspace's event stream open until `stop`.
 * Deduplicates events by `event_id` within a bounded window, since a reconnect can repeat recent ones.
 */
export class MulticaEvents {
  private socket: SocketLike | undefined
  private attempt = 0
  private stopped = true
  private timer: ReturnType<typeof setTimeout> | undefined
  private ping: ReturnType<typeof setInterval> | undefined
  private readonly seen: string[] = []
  private readonly seenSet = new Set<string>()
  constructor(private readonly connection: MulticaConnection, private readonly listener: MulticaEventsListener,
    private readonly socketFactory: SocketFactory = url => new WebSocket(url) as unknown as SocketLike) {}
  /** Whether the stream is connected and authenticated now. */
  live = false
  start(): void {
    if (!this.stopped) return
    this.stopped = false
    this.open()
  }
  stop(): void {
    this.stopped = true
    this.live = false
    clearTimeout(this.timer); clearInterval(this.ping)
    const socket = this.socket
    this.socket = undefined
    socket?.close()
  }
  private open(): void {
    const url = new URL('/ws', this.connection.serverUrl.replace(/^http/, 'ws'))
    url.searchParams.set('workspace_id', this.connection.workspaceId)
    url.searchParams.set('client_platform', 'dsh-superagent')
    let socket: SocketLike
    try { socket = this.socketFactory(url.toString()) } catch (error) { this.retry(String(error)); return }
    this.socket = socket
    let authed = false
    const authTimer = setTimeout(() => { if (!authed) socket.close() }, AUTH_TIMEOUT_MS)
    socket.onopen = () => { socket.send(JSON.stringify({ type: 'auth', payload: { token: this.connection.token } })) }
    socket.onmessage = ({ data }) => {
      if (this.socket !== socket) return
      let message: MulticaEvent
      try { message = JSON.parse(String(data)) as MulticaEvent } catch { return }
      if (!authed) {
        if (message.type !== 'auth_ack') { socket.close(); return }
        authed = true; clearTimeout(authTimer)
        this.attempt = 0; this.live = true
        clearInterval(this.ping)
        this.ping = setInterval(() => { if (socket.readyState === 1) socket.send(JSON.stringify({ type: 'ping' })) }, PING_MS)
        this.listener.connected()
        return
      }
      if (message.type === 'pong' || message.type.endsWith('_ack')) return
      if (message.event_id) {
        if (this.seenSet.has(message.event_id)) return
        this.seenSet.add(message.event_id); this.seen.push(message.event_id)
        if (this.seen.length > 2000) this.seenSet.delete(this.seen.shift()!)
      }
      this.listener.event({ ...message, payload: message.payload ?? {} })
    }
    socket.onerror = () => { /* onclose follows and reconnects. */ }
    socket.onclose = event => {
      clearTimeout(authTimer)
      if (this.socket !== socket) return
      this.socket = undefined
      const reason = authed ? `closed (${event.code ?? '?'}${event.reason ? ` ${event.reason}` : ''})` : 'authentication failed or timed out'
      this.live = false; clearInterval(this.ping)
      this.listener.disconnected(reason)
      this.retry(reason)
    }
  }
  private retry(_reason: string): void {
    // The reason has already gone to the listener; reconnect after the next backoff step.
    if (this.stopped) return
    const delay = BACKOFF_MS[Math.min(this.attempt++, BACKOFF_MS.length - 1)]!
    this.timer = setTimeout(() => { if (!this.stopped) this.open() }, delay)
  }
}
