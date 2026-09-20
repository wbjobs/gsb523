const textEncoder = new TextEncoder();
const textDecoder = new TextDecoder();

export function fnv1aHex(str) {
  const bytes = textEncoder.encode(str);
  let hash = 0x811c9dc5;
  for (let i = 0; i < bytes.length; i++) {
    hash ^= bytes[i];
    hash = Math.imul(hash, 0x01000193) >>> 0;
  }
  return hash.toString(16).padStart(8, '0');
}

export function checksum(bytesOrString) {
  const bytes = typeof bytesOrString === 'string' ? textEncoder.encode(bytesOrString) : bytesOrString;
  let hash = 0x811c9dc5;
  for (let i = 0; i < bytes.length; i++) {
    hash ^= bytes[i];
    hash = Math.imul(hash, 0x01000193) >>> 0;
  }
  return hash.toString(16).padStart(8, '0');
}

export async function gzip(bytes) {
  const cs = new CompressionStream('gzip');
  const stream = new Blob([bytes]).stream().pipeThrough(cs);
  return new Uint8Array(await new Response(stream).arrayBuffer());
}

export async function gunzip(bytes) {
  const ds = new DecompressionStream('gzip');
  const stream = new Blob([bytes]).stream().pipeThrough(ds);
  return new Uint8Array(await new Response(stream).arrayBuffer());
}

export function encodeJson(value) {
  return textEncoder.encode(JSON.stringify(value));
}

export function decodeJson(bytes) {
  return JSON.parse(textDecoder.decode(bytes));
}

export function newId() {
  return crypto.randomUUID();
}

export function deepClone(value) {
  return JSON.parse(JSON.stringify(value));
}

export function byteLengthOf(value) {
  return value == null ? 0 : textEncoder.encode(JSON.stringify(value)).length;
}

export function isRollbackMarker(type) {
  return typeof type === 'string' && type.charAt(0) === '$';
}

export function compileFunctionSource(source, argNames) {
  let fn;
  const args = [...argNames, `"use strict"; return (${source});`];
  // eslint-disable-next-line no-new-func
  fn = new Function(...args)();
  if (typeof fn !== 'function') throw new TypeError('source did not compile to a function');
  return fn;
}
