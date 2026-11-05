/** The Multica event stream: first-frame auth, deduplication, and reconnection with a catch-up. */
import { afterEach, expect, it, vi } from 'vitest'
import { MulticaEvents, type SocketLike } from '../src/executor/multica-events.ts'

afterEach(() => { vi.useRealTimers() })

function fakeSockets() {
  const sockets: (SocketLike & { sent: string[]; url: string })[] = []
  const factory = (url: string) => {
    const socket = { url, sent: [] as string[], readyState: 1, onopen: null, onmessage: null, onclose: null, onerror: null,
      send(data: string) { this.sent.push(data) }, close() { this.onclose?.({ code: 1000 }) } } as SocketLike & { sent: string[]; url: string }
    sockets.push(socket)
    return socket
  }
  return { sockets, factory }
}
const frame = (socket: SocketLike, value: object) => { socket.onmessage?.({ data: JSON.stringify(value) }) }

it('authenticates with the first frame, reports events once each, and catches up after every reconnect', () => {
  vi.useFakeTimers()
  const { sockets, factory } = fakeSockets()
  const listener = { connected: vi.fn(), event: vi.fn(), disconnected: vi.fn() }
  const events = new MulticaEvents({ serverUrl: 'https://api.multica.test', workspaceId: 'w1', token: 'mul_secret' }, listener, factory)
  events.start()
  const first = sockets[0]!
  expect(first.url).toBe('wss://api.multica.test/ws?workspace_id=w1&client_platform=dsh-superagent')
  first.onopen?.({})
  expect(JSON.parse(first.sent[0]!)).toEqual({ type: 'auth', payload: { token: 'mul_secret' } })
  frame(first, { type: 'auth_ack' })
  expect(listener.connected).toHaveBeenCalledTimes(1)
  const event = { type: 'task:completed', event_id: 'e1', payload: { issue_id: 'i1' } }
  frame(first, event); frame(first, event); frame(first, { type: 'pong' })
  expect(listener.event).toHaveBeenCalledTimes(1)

  first.onclose?.({ code: 1006 })
  expect(listener.disconnected).toHaveBeenCalledWith('closed (1006)')
  vi.advanceTimersByTime(1000)
  const second = sockets[1]!
  second.onopen?.({})
  frame(second, { type: 'auth_ack' })
  expect(listener.connected).toHaveBeenCalledTimes(2)
  events.stop()
  vi.advanceTimersByTime(120_000)
  expect(sockets).toHaveLength(2)
})

it('treats anything but auth_ack as a failed authentication, and backs off', () => {
  vi.useFakeTimers()
  const { sockets, factory } = fakeSockets()
  const listener = { connected: vi.fn(), event: vi.fn(), disconnected: vi.fn() }
  const events = new MulticaEvents({ serverUrl: 'https://api.multica.test', workspaceId: 'w1', token: 'bad' }, listener, factory)
  events.start()
  sockets[0]!.onopen?.({})
  frame(sockets[0]!, { error: 'invalid token' })
  expect(listener.connected).not.toHaveBeenCalled()
  expect(listener.disconnected).toHaveBeenCalledWith('authentication failed or timed out')
  vi.advanceTimersByTime(999)
  expect(sockets).toHaveLength(1)
  vi.advanceTimersByTime(1)
  expect(sockets).toHaveLength(2)
  events.stop()
})
