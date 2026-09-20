import { test } from 'node:test';
import assert from 'node:assert/strict';
import { createStore } from './helpers.js';

test('rollback forks epoch, hides pre-rollback events, keeps time travel', async () => {
  const { store } = await createStore({ snapshotEvery: 0 });
  for (let i = 0; i < 5; i++) await store.append('counter', 'inc', { n: 1 });
  assert.equal((await store.getState('counter')).count, 5);

  const before5 = store.physicalSeq;
  await store.rollback('counter', 3);

  const head = store.getHead('counter');
  assert.equal(head.epoch, 1);
  assert.equal(head.seq, 0);

  const state = await store.getState('counter');
  assert.equal(state.count, 3);

  const visible = await store.query({ stream: 'counter' });
  assert.equal(visible.length, 3);
  assert.deepEqual(visible.map((e) => e.seq), [1, 2, 3]);
  // events #1..#3 are the inherited prefix of the forked epoch
  assert.deepEqual(visible.map((e) => e.epoch), [0, 0, 0]);

  // Continue the forked stream: seq restarts at 1 in epoch 1.
  await store.append('counter', 'inc', { n: 10 });
  const after = await store.getState('counter');
  assert.equal(after.count, 13);

  const head2 = store.getHead('counter');
  assert.equal(head2.epoch, 1);
  assert.equal(head2.seq, 1);

  // Time travel to the pre-rollback world.
  const oldVisible = await store.query({ stream: 'counter', atGlobalSeq: before5 });
  assert.equal(oldVisible.length, 5);
  assert.ok(oldVisible.every((e) => e.epoch === 0));
  const oldState = await store.getStateAt('counter', before5);
  assert.equal(oldState.count, 5);

  // System markers stay hidden unless explicitly requested.
  const withMarkers = await store.query({ stream: 'counter', includeSystem: true, limit: 1000 });
  assert.ok(withMarkers.some((e) => e.type === '$rollback'));
});

test('can rollback to seq 0 and rebuild empty state', async () => {
  const { store } = await createStore({ snapshotEvery: 2 });
  for (let i = 0; i < 4; i++) await store.append('counter', 'inc');
  await store.rollback('counter', 0);
  const state = await store.getState('counter');
  assert.equal(state.count, 0);
  await store.append('counter', 'set', { value: 99 });
  assert.equal((await store.getState('counter')).count, 99);
});

test('rollback rejects target at/after head and unknown streams', async () => {
  const { store } = await createStore();
  await assert.rejects(() => store.rollback('nope', 0), /unknown\/empty/);
  await store.append('counter', 'inc');
  await assert.rejects(() => store.rollback('counter', 1), /not before head/);
});

test('verifyStream passes across rollback boundaries', async () => {
  const { store } = await createStore({ snapshotEvery: 0 });
  for (let i = 0; i < 6; i++) await store.append('counter', 'inc');
  await store.rollback('counter', 4);
  await store.append('counter', 'dec');
  const result = await store.verifyStream('counter');
  assert.equal(result.ok, true);
  assert.ok(result.physicalEvents >= 8);
});
