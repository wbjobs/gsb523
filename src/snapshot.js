import { gzip, gunzip, encodeJson, decodeJson, checksum, newId } from './util.js';
import { SnapshotCorruptError } from './errors.js';

/**
 * Snapshot record layout:
 * {
 *   snapshotId, stream, epoch, baseSeq,
 *   timestamp, compressed: boolean,
 *   state: Uint8Array (gzip+json) or raw JSON-serializable value,
 *   stateChecksum, rawBytes, compressedBytes, eventHash
 * }
 *
 * eventHash anchors the snapshot: it is the prevHash of the last included
 * event, so a restored snapshot plus the subsequent replay is bit-identical
 * to replaying the whole stream.
 */
export async function encodeSnapshot({ stream, epoch, baseSeq, state, eventHash, compress }) {
  const jsonBytes = encodeJson(state);
  const stateChecksum = checksum(jsonBytes);
  const rawBytes = jsonBytes.length;
  let payload = jsonBytes;
  let compressed = false;
  if (compress) {
    payload = await gzip(jsonBytes);
    compressed = true;
  }
  return {
    snapshotId: newId(),
    stream,
    epoch,
    baseSeq,
    timestamp: Date.now(),
    compressed,
    state: payload,
    stateChecksum,
    rawBytes,
    compressedBytes: payload.length,
    eventHash
  };
}

export async function decodeSnapshot(record) {
  let jsonBytes;
  if (record.compressed) {
    try {
      jsonBytes = await gunzip(record.state);
    } catch (error) {
      throw new SnapshotCorruptError(record.snapshotId);
    }
  } else if (record.state instanceof Uint8Array) {
    jsonBytes = record.state;
  } else {
    jsonBytes = encodeJson(record.state);
  }
  if (checksum(jsonBytes) !== record.stateChecksum) {
    throw new SnapshotCorruptError(record.snapshotId);
  }
  return decodeJson(jsonBytes);
}

export function snapshotStoredBytes(record) {
  const stateBytes =
    record.state instanceof Uint8Array
      ? record.state.length
      : encodeJson(record.state).length;
  return stateBytes + 96;
}
