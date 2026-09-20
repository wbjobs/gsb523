import { test } from 'node:test';
import assert from 'node:assert/strict';
import { createStore } from './helpers.js';
import { VersionConflictError } from '../src/errors.js';

test('concurrent appends are serialized and never lose updates', async () => {
  const { store } = await createStore({ snapshotEvery: 0 });
  const N = 200;
  const tasks = [];
  for (let i = 0; i < N; i++) {
    tasks.push(store.append('counter', 'inc', { n: 1 }));
  }
  const envelopes = await Promise.all(tasks);
  const seqs = envelopes.map((e) => e.seq).sort((a, b) => a - b);
  assert.deepEqual(seqs, Array.from({ length: N }, (_, i) => i + 1));
  const globalSeqs = new Set(envelopes.map((e) => e.globalSeq));
  assert.equal(globalSeqs.size, N);
  assert.equal((await store.getState('counter')).count, N);
});

test('optimistic version conflict is reported with expected/actual', async () => {
  const { store } = await createStore();
  await store.append('counter', 'inc');
  await store.append('counter', 'inc');
  await assert.rejects(
    () => store.append('counter', 'inc', null, 1),
    (err) =>
      err instanceof VersionConflictError &&
      err.code === 'VERSION_CONFLICT' &&
      err.expected === 1 &&
      err.actual === 2
  );
});

test('expectedVersion -1 enforces stream creation once', async () => {
  const { store } = await createStore();
  await store.append('counter', 'set', { value: 1 }, -1);
  await assert.rejects(() => store.append('counter', 'set', { value: 2 }, -1), VersionConflictError);
});

test('no dirty reads: a concurrent read never observes a partial batch', async () => {
  const { store, adapter } = await createStore({ snapshotEvery: 0 });
  for (let i = 0; i < 50; i++) await store.append('counter', 'inc');

  // Instrument the adapter: while the bulk transaction is open, readers
  // using the same snapshot must not see uncommitted rows. The memory
  // adapter gives each transaction its own snapshot, so we assert at the
  // engine level by racing a getState during appendStream.
  const observations = [];
  const BATCH = 500;
  const bulk = store.appendStream(
    'counter',
    Array.from({ length: BATCH }, () => ({ type: 'inc' })),
    { expectedVersion: 50, batchSize: 500 }
  );
  const readers = [];
  for (let i = 0; i < 10; i++) {
    readers.push(
      (async () => {
        const state = await store.getState('counter');
        observations.push(state.count);
      })()
    );
  }
  await Promise.all([bulk, ...readers]);
  for (const value of observations) {
    assert.ok(
      value === 50 || value === 50 + BATCH,
      `read observed torn value ${value} (allowed: 50 or ${50 + BATCH})`
    );
  }
  assert.equal((await store.getState('counter')).count, 50 + BATCH);
  void adapter;
});

test('concurrent writers racing on one expected version: exactly one wins', async () => {
  const { store } = await createStore();
  await store.append('counter', 'set', { value: 0 });
  const attempts = await Promise.allSettled(
    Array.from({ length: 20 }, () => store.append('counter', 'inc', null, 1))
  );
  const fulfilled = attempts.filter((a) => a.status === 'fulfilled');
  const rejected = attempts.filter((a) => a.status === 'rejected');
  assert.equal(fulfilled.length, 1);
  assert.equal(rejected.length, 19);
  assert.ok(rejected.every((a) => a.reason.code === 'VERSION_CONFLICT'));
  assert.equal((await store.getState('counter')).count, 1);
});
