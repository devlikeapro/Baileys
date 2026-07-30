import WebSocket from 'ws'
import { DEFAULT_ORIGIN } from '../../Defaults'
import { AbstractSocketClient } from './types'

// how long to wait for the close handshake before destroying the underlying TCP socket
const CLOSE_TIMEOUT_MS = 5_000

export class WebSocketClient extends AbstractSocketClient {
	protected socket: WebSocket | null = null

	get isOpen(): boolean {
		return this.socket?.readyState === WebSocket.OPEN
	}
	get isClosed(): boolean {
		return this.socket === null || this.socket?.readyState === WebSocket.CLOSED
	}
	get isClosing(): boolean {
		return this.socket === null || this.socket?.readyState === WebSocket.CLOSING
	}
	get isConnecting(): boolean {
		return this.socket?.readyState === WebSocket.CONNECTING
	}

	connect() {
		if (this.socket) {
			return
		}

		this.socket = new WebSocket(this.url, {
			origin: DEFAULT_ORIGIN,
			headers: this.config.options?.headers as {},
			handshakeTimeout: this.config.connectTimeoutMs,
			timeout: this.config.connectTimeoutMs,
			agent: this.config.agent
		})

		this.socket.setMaxListeners(0)

		const events = ['close', 'error', 'upgrade', 'message', 'open', 'ping', 'pong', 'unexpected-response']

		for (const event of events) {
			this.socket?.on(event, (...args: any[]) => this.emit(event, ...args))
		}
	}

	async close() {
		const socket = this.socket
		if (!socket) {
			return
		}

		this.socket = null
		// 'close' has already fired: ws.close() would be a no-op and the event will never come again
		if (socket.readyState === WebSocket.CLOSED) {
			return
		}

		await new Promise<void>(resolve => {
			// on a half-open TCP connection the peer never answers the close frame,
			// so destroy the socket after a timeout; terminate() emits 'close' too, resolving the same way
			const timer = setTimeout(() => socket.terminate(), CLOSE_TIMEOUT_MS)
			socket.once('close', () => {
				clearTimeout(timer)
				resolve()
			})
			socket.close()
		})
	}
	send(str: string | Uint8Array, cb?: (err?: Error) => void): boolean {
		this.socket?.send(str, cb)

		return Boolean(this.socket)
	}
}
