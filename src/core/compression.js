import { stableStringify } from './canonical.js';

const encoder = new TextEncoder();
const decoder = new TextDecoder();

export async function compressJson(value) {
  const json = stableStringify(value);
  const bytes = encoder.encode(json);

  if (typeof CompressionStream !== 'undefined') {
    try {
      const compressed = await gzip(bytes);
      return {
        codec: 'gzip+json',
        bytes: compressed,
        originalBytes: bytes.byteLength,
        compressed: true,
      };
    } catch {
      // Compression is an optimization. JSON remains the durability fallback.
    }
  }

  return {
    codec: 'json',
    bytes,
    originalBytes: bytes.byteLength,
    compressed: false,
  };
}

export async function decompressJson(snapshot) {
  const bytes = snapshot.stateBytes;

  if (snapshot.codec === 'gzip+json') {
    const plain = await gunzip(bytes);
    return JSON.parse(decoder.decode(plain));
  }

  if (snapshot.codec === 'json') {
    return JSON.parse(decoder.decode(bytes));
  }

  throw new Error(`Unsupported snapshot codec: ${snapshot.codec}`);
}

async function gzip(bytes) {
  const stream = new Blob([bytes]).stream().pipeThrough(
    new CompressionStream('gzip')
  );
  return new Uint8Array(await new Response(stream).arrayBuffer());
}

async function gunzip(bytes) {
  if (typeof DecompressionStream === 'undefined') {
    throw new Error('DecompressionStream is not available');
  }
  const stream = new Blob([bytes]).stream().pipeThrough(
    new DecompressionStream('gzip')
  );
  return new Uint8Array(await new Response(stream).arrayBuffer());
}
