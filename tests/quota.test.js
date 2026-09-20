import { test } from 'node:test';
import assert from 'node:assert/strict';
import { createStore } from './helpers.js';
import { QuotaExceededError, ReadOnlyError } from '../src/errors.js';

test('hard quota rejects writes with QuotaExceededError but keeps reads working', async () => {
  const { store } = await createStore({
    snapshotEvery: 0,
    quota: { maxBytes: 4000, softRatio: 0.99 }
  });
  let blockedAt = -1;
  for (let i = 0; i < 200; i++) {
    try {
      await store.append('counter', 'inc', { padding: 'p'.repeat(60) });
    } catch (error) {
      assert.ok(error instanceof QuotaExceededError);
      assert.equal(error.code, 'QUOTA_EXCEEDED');
      blockedAt = i;
      break;
    }
  }
  assert.ok(blockedAt > 0);

  // The store is degraded: subsequent writes still fail.
  await assert.rejects(() => store.append('counter', 'inc'), (err) =>
    ['QUOTA_EXCEEDED', 'READ_ONLY'].includes(err.code)
  );

  // Reads continue to work.
  const state = await store.getState('counter');
  assert.equal(typeof state.count, 'number');
  const rows = await store.query({ stream: 'counter', limit: 100000 });
  assert.equal(rows.length, blockedAt);
});

test('soft threshold triggers snapshot compaction automatically', async () => {
  const { store, adapter } = await createStore({
    snapshotEvery: 1,
    quota: { maxBytes: 600, softRatio: 0.5, snapshotsPerStream: 2 }
  });
  store.registerReducer('counter', (state, event) => {
    if (state == null) state = { count: 0, log: [] };
    if (event.type === 'inc') state.count += 1;
    state.log.push('s'.repeat(40));
    return state;
  });
  for (let i = 0; i < 10; i++) await store.append('counter', 'inc');
  const snaps = await adapter.runTransaction(['snapshots'], 'readonly', (tx) =>
    tx.store('snapshots').allSnapshots()
  );
  assert.ok(snaps.length <= 2, `GC should keep at most 2 snapshots, got ${snaps.length}`);
  assert.equal((await store.getState('counter')).count, 10);
});

test('forced read-only mode rejects all writes and resumes on demand', async () => {
  const { store } = await createStore({ snapshotEvery: 0 });
  store.quota.enterReadOnly();
  await assert.rejects(() => store.append('counter', 'inc'), ReadOnlyError);
  store.quota.resume();
  const event = await store.append('counter', 'inc');
  assert.equal(event.seq, 1);
});

test('backing QuotaExceededError is mapped onto the degradation policy', async () => {
  const { store, adapter } = await createStore({ snapshotEvery: 0 });
  adapter.injectFailure('QuotaExceededError');
  await assert.rejects(() => store.append('counter', 'inc'), (err) => {
    return err instanceof QuotaExceededError && err.code === 'QUOTA_EXCEEDED';
  });
});
