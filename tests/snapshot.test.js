import { test } from 'node:test';
import assert from 'node:assert/strict';
import { createStore } from './helpers.js';
import { decodeSnapshot } from '../src/snapshot.js';

test('snapshots are gzip compressed with checksum and match full replay', async () => {
  const { store, adapter } = await createStore({
    snapshotEvery: 500,
    extraReducers: {
      big: (state, event) => {
        if (state == null) state = { entries: [] };
        state.entries.push('x'.repeat(64) + event.seq);
        return state;
      }
    }
  });
  for (let i = 0; i < 1500; i++) {
    await store.append('counter', i % 2 === 0 ? 'inc' : 'dec', { n: 1 });
  }
  for (let i = 0; i < 1200; i++) await store.append('big', 'add');
  const snapshots = await adapter.runTransaction(['snapshots'], 'readonly', (tx) =>
    tx.store('snapshots').allSnapshots()
  );
  assert.ok(snapshots.length >= 2, 'periodic snapshots should exist');
  const big = snapshots.filter((snap) => snap.stream === 'big');
  assert.ok(big.length >= 1);
  for (const snap of big) {
    assert.equal(snap.compressed, true);
    assert.ok(snap.compressedBytes < snap.rawBytes, 'gzip should shrink repetitive state');
    const decoded = await decodeSnapshot(snap);
    assert.ok(Array.isArray(decoded.entries));
  }
  const replay = await store.getState('counter');
  assert.equal(replay.count, 0);
});

test('corrupted snapshot bytes are detected by checksum', async () => {
  const { store, adapter } = await createStore({ snapshotEvery: 10 });
  for (let i = 0; i < 10; i++) await store.append('counter', 'inc');
  const corrupted = await adapter.runTransaction(['snapshots'], 'readwrite', async (tx) => {
    const snaps = await tx.store('snapshots').allSnapshots();
    const snap = snaps[snaps.length - 1];
    const bytes = snap.state;
    bytes[0] = bytes[0] ^ 0xff;
    await tx.store('snapshots').put(snap);
    return snap.snapshotId;
  });
  await assert.rejects(
    () => store.verifyStream('counter'),
    (err) => err.code === 'SNAPSHOT_CORRUPT'
  );
  void corrupted;
});

test('snapshot after rollback restores the forked baseline correctly', async () => {
  const { store } = await createStore({ snapshotEvery: 3 });
  for (let i = 0; i < 9; i++) await store.append('counter', 'inc');
  await store.rollback('counter', 4);
  await store.append('counter', 'inc', { n: 100 });
  const state = await store.getState('counter');
  assert.equal(state.count, 104);

  // Re-materialize from scratch path via forced snapshot.
  await store.snapshot('counter');
  const state2 = await store.getState('counter');
  assert.deepEqual(state2, state);
});

test('snapshot compaction keeps reachable snapshots and frees bytes', async () => {
  const { store, adapter } = await createStore({ snapshotEvery: 0 });
  for (let i = 0; i < 8; i++) {
    await store.append('counter', 'inc');
    await store.snapshot('counter'); // automatic GC is disabled; collect snapshots
  }
  const before = await adapter.runTransaction(['snapshots'], 'readonly', (tx) =>
    tx.store('snapshots').allSnapshots()
  );
  assert.equal(before.length, 8);
  const freed = await store.quota.compactSnapshots('counter');
  assert.ok(freed > 0);
  const after = await adapter.runTransaction(['snapshots'], 'readonly', (tx) =>
    tx.store('snapshots').allSnapshots()
  );
  assert.ok(after.length <= store.quota.snapshotsPerStream);
  // State still correct after GC.
  assert.equal((await store.getState('counter')).count, 8);
});

test('state survives reopening the store against the same adapter', async () => {
  const { store, adapter } = await createStore({ snapshotEvery: 2 });
  for (let i = 0; i < 6; i++) await store.append('counter', 'inc');
  await store.rollback('counter', 2);
  await store.append('counter', 'inc', { n: 5 });

  const { EventStore } = await import('../src/engine.js');
  const reopened = new EventStore(adapter, { snapshotEvery: 2 });
  reopened.registerReducer('counter', (state, event) => {
    if (state == null) return { count: 0 };
    if (event.type === 'inc') state.count += (event.payload && event.payload.n) || 1;
    return state;
  });
  await reopened.open();
  assert.equal(reopened.getHead('counter').epoch, 1);
  assert.equal((await reopened.getState('counter')).count, 7);
});
