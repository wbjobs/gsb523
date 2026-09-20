import assert from 'node:assert/strict';
import { test } from 'node:test';
import { EventStore } from '../src/core/event-store.js';
import { MemoryStorage } from '../src/core/memory-storage.js';
import { createDefaultReducer } from '../src/core/reducers.js';

const TOTAL = 100_000;
const BATCH = 5_000;

test('100k events satisfy query and snapshot time-travel budget', async () => {
  const store = new EventStore(new MemoryStorage(), {
    snapshotEvery: 0,
  });
  store.configureReducer('*', createDefaultReducer());

  const streamId = 'perf';
  const writeStart = performance.now();

  for (let offset = 0; offset < TOTAL; offset += BATCH) {
    const count = Math.min(BATCH, TOTAL - offset);
    await store.appendMany(
      streamId,
      Array.from({ length: count }, (_, index) => ({
        type: 'counter.incremented',
        entityId: 'counter',
        payload: { delta: 1, n: offset + index },
      })),
      { snapshot: false }
    );
  }

  const writeMs = performance.now() - writeStart;

  const queryStart = performance.now();
  const queried = await store.queryEvents({
    streamId,
    limit: TOTAL,
  });
  const queryMs = performance.now() - queryStart;

  assert.equal(queried.items.length, TOTAL);
  assert.ok(queryMs < 1000, `100k indexed range query took ${queryMs.toFixed(2)}ms`);

  const snapshotStart = performance.now();
  const snapshot = await store.createSnapshot(streamId);
  const snapshotMs = performance.now() - snapshotStart;

  const restoreStart = performance.now();
  const restored = await store.materialize(streamId);
  const restoreMs = performance.now() - restoreStart;
  assert.equal(restored.state.entities.counter.count, TOTAL);
  assert.equal(restored.applied, 0);

  const travelStart = performance.now();
  const traveled = await store.materialize(streamId, {
    atVersion: 50_000,
  });
  const travelMs = performance.now() - travelStart;
  assert.equal(traveled.state.entities.counter.count, 50_000);
  assert.ok(travelMs < 1000, `100k time-travel query took ${travelMs.toFixed(2)}ms`);

  console.table([
    { operation: `batch append ${TOTAL}`, ms: round(writeMs) },
    { operation: 'indexed event range query', ms: round(queryMs) },
    { operation: 'gzip full-state snapshot', ms: round(snapshotMs) },
    { operation: 'snapshot restore at head', ms: round(restoreMs) },
    { operation: 'time travel to 50k', ms: round(travelMs) },
  ]);

  console.log({
    codec: snapshot.codec,
    originalBytes: snapshot.originalBytes,
    compressedBytes: snapshot.compressedBytes,
    ratio: `${(snapshot.compressedBytes / snapshot.originalBytes).toFixed(3)}`,
  });
});

function round(value) {
  return Number(value.toFixed(2));
}
