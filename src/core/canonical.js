export function stableStringify(value) {
  return JSON.stringify(prepareCanonical(value));
}

function prepareCanonical(value) {
  if (value === null || typeof value !== 'object') return value;
  if (Array.isArray(value)) return value.map(prepareCanonical);

  const result = {};
  for (const key of Object.keys(value).sort()) {
    const item = value[key];
    if (item !== undefined) result[key] = prepareCanonical(item);
  }
  return result;
}

export function fnv1a64(input) {
  const bytes = new TextEncoder().encode(input);
  let hash = 0xcbf29ce484222325n;
  const prime = 0x100000001b3n;
  const mask = 0xffffffffffffffffn;

  for (const byte of bytes) {
    hash ^= BigInt(byte);
    hash = BigInt.asUintN(64, hash * prime);
  }

  return hash.toString(16).padStart(16, '0');
}

export function hashValue(value) {
  return fnv1a64(stableStringify(value));
}

export function hashEvent(event) {
  const { hash, ...integrityInput } = event;
  return hashValue(integrityInput);
}
