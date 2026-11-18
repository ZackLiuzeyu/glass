// Exercises the browser transport against the real local bridge in --test mode.
import { readFileSync } from 'node:fs';
import { WsTransport } from '../../web/ws-transport.js';

const token = readFileSync(process.env.GLASS_TEST_TOKEN_FILE, 'utf8').trim();
const transport = new WsTransport(process.env.GLASS_TEST_WS_URL ?? 'ws://127.0.0.1:18080/tcp-bridge');
const timeout = setTimeout(() => { console.error('TCP bridge client timed out'); transport.close(); process.exitCode = 1; }, 12_000);

await transport.ready;
const control = await transport.createBidirectionalStream();
const writer = control.writable.getWriter();
const body = new TextEncoder().encode(JSON.stringify({
  t: 'hello', token, device: 'TCP bridge browser transport test',
  codecs: ['h264'], ua: 'node-test', cursor: false, acks: true,
}));
const frame = new Uint8Array(4 + body.length);
new DataView(frame.buffer).setUint32(0, body.length, true);
frame.set(body, 4);
await writer.write(frame);

const reader = control.readable.getReader();
let buffered = new Uint8Array();
let welcomed = false;
while (!welcomed) {
  const { value, done } = await reader.read();
  if (done) throw new Error('control stream closed before welcome');
  const next = new Uint8Array(buffered.length + value.length);
  next.set(buffered); next.set(value, buffered.length); buffered = next;
  if (buffered.length < 4) continue;
  const size = new DataView(buffered.buffer).getUint32(0, true);
  if (buffered.length < 4 + size) continue;
  const msg = JSON.parse(new TextDecoder().decode(buffered.subarray(4, 4 + size)));
  if (msg.t === 'denied') throw new Error('host denied test token');
  welcomed = msg.t === 'welcome';
}

const dgrams = transport.datagrams.createWritable().getWriter();
await dgrams.write(new Uint8Array([4, 0])); // request keyframe
const video = transport.datagrams.readable.getReader();
for (;;) {
  const { value, done } = await video.read();
  if (done) throw new Error('datagrams ended before video');
  if (value[0] === 1) break;
}
clearTimeout(timeout);
console.log('Browser transport authenticated and received video');
transport.close();
