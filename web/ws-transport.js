// WebTransport-shaped transport over a single WebSocket. Tailscale Serve can
// forward this HTTPS/TCP connection to a userspace Tailscale host without
// claiming the machine's VPN interface. The host bridges it to Glass locally.
const DATAGRAM = 0;
const OPEN_BI = 1;
const OPEN_UNI = 2;
const DATA = 3;
const END = 4;
const INCOMING_UNI = 5;
const MAX_BUFFERED = 512 * 1024;

export class WsTransport {
  constructor(url) {
    this.ws = new WebSocket(url);
    this.ws.binaryType = 'arraybuffer';
    this.streams = new Map();
    this.nextId = 1;
    this.ended = false;
    this.incomingUnidirectionalStreams = new ReadableStream({
      start: (controller) => { this.incoming = controller; },
    });
    const readable = new ReadableStream({
      start: (controller) => { this.datagramIn = controller; },
    }, { highWaterMark: 256 });
    const writable = new WritableStream({
      write: (chunk) => { if (this.ws.bufferedAmount < MAX_BUFFERED) this.send(DATAGRAM, 0, chunk); },
    });
    this.datagrams = { readable, writable, createWritable: () => writable, maxDatagramSize: 1200 };
    this.ready = new Promise((resolve, reject) => { this.resolveReady = resolve; this.rejectReady = reject; });
    this.closed = new Promise((resolve) => { this.resolveClosed = resolve; });
    this.ready.catch(() => {});
    this.ws.onopen = () => this.resolveReady();
    this.ws.onmessage = ({ data }) => this.onMessage(data);
    this.ws.onerror = () => this.end('WebSocket failed');
    this.ws.onclose = () => this.end('WebSocket closed');
  }

  send(kind, id, payload = new Uint8Array()) {
    if (this.ended || this.ws.readyState !== WebSocket.OPEN) throw new Error('connection closed');
    const frame = new Uint8Array(5 + payload.byteLength);
    frame[0] = kind;
    new DataView(frame.buffer).setUint32(1, id);
    frame.set(payload, 5);
    this.ws.send(frame);
  }

  stream(id) {
    let state = this.streams.get(id);
    if (!state) this.streams.set(id, (state = {}));
    return state;
  }

  readableFor(id) {
    const state = this.stream(id);
    return new ReadableStream({ start: (controller) => { state.controller = controller; } });
  }

  writableFor(id) {
    return new WritableStream({
      write: async (chunk) => {
        while (!this.ended && this.ws.bufferedAmount > MAX_BUFFERED) {
          await new Promise((resolve) => setTimeout(resolve, 5));
        }
        this.send(DATA, id, chunk);
      },
      close: () => this.send(END, id),
    });
  }

  onMessage(data) {
    if (!(data instanceof ArrayBuffer) || data.byteLength < 5) { this.end('bad bridge packet'); return; }
    const bytes = new Uint8Array(data);
    const kind = bytes[0];
    const id = new DataView(data).getUint32(1);
    const payload = bytes.subarray(5);
    switch (kind) {
      case DATAGRAM:
        if (this.datagramIn.desiredSize > 0) this.datagramIn.enqueue(payload);
        break;
      case INCOMING_UNI:
        this.incoming.enqueue(this.readableFor(id));
        break;
      case DATA:
        this.streams.get(id)?.controller?.enqueue(payload);
        break;
      case END:
        try { this.streams.get(id)?.controller?.close(); } catch {}
        this.streams.delete(id);
        break;
      default:
        this.end('bad bridge packet kind');
    }
  }

  async createBidirectionalStream() {
    if (this.ended) throw new Error('connection closed');
    const id = this.nextId++;
    this.send(OPEN_BI, id);
    return { readable: this.readableFor(id), writable: this.writableFor(id) };
  }

  async createUnidirectionalStream() {
    if (this.ended) throw new Error('connection closed');
    const id = this.nextId++;
    this.send(OPEN_UNI, id);
    return this.writableFor(id);
  }

  end(reason) {
    if (this.ended) return;
    this.ended = true;
    this.rejectReady(new Error(reason));
    for (const state of this.streams.values()) {
      try { state.controller?.error(new Error(reason)); } catch {}
    }
    this.streams.clear();
    try { this.datagramIn.close(); } catch {}
    try { this.incoming.close(); } catch {}
    this.resolveClosed({ reason });
    if (this.ws.readyState === WebSocket.OPEN) this.ws.close();
  }

  close() { this.end('closed'); }
}
