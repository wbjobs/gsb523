import assert from 'node:assert/strict';
import { test } from 'node:test';
import { EventStore } from '../src/core/event-store.js';
import { MemoryStorage, OverflowStorage } from '../src/core/memory-storage.js';
import { ConcurrencyConflictError } from '../src/core/errors.js';
import { createDefaultReducer } from '../src/core/reducers.js';

async function createStore(options = {}) {
  const store = new EventStore(
    options.storage || new MemoryStorage(),
    {
      snapshotEvery: 0,
      ...options,
    }
  );
  store.configureReducer('*', createDefaultReducer());
  return store;
}

test('appends immutable events with monotonic per-stream versions', async () => {
  const store = await createStore();

  const first = await store.append('orders', 'order.opened', { total: 10 }, {
    entityId: 'order-1',
    expectedVersion: -1,
  });
  const second = await store.append('orders', 'order.paid', { method: 'card' }, {
    entityId: 'order-1',
    expectedVersion: 1,
  });

  assert.equal(first.version, 1);
  assert.equal(second.version, 2);
  assert.equal(second.parentId, first.id);
  assert.equal(second.parentHash, first.hash);
  assert.match(first.hash, /^[0-9a-f]{16}$/);

  const events = await store.queryEvents({ streamId: 'orders' });
  assert.equal(events.count, 2);
});

test('rejects stale expected versions and avoids dirty reads', async () => {
  const store = await createStore();
  await store.append('accounts', 'account.opened', {}, {
    entityId: 'a',
    expectedVersion: -1,
  });

  await assert.rejects(
    () => store.append('accounts', 'account.changed', {}, {
      entityId: 'a',
      expectedVersion: 0,
    }),
    (error) => error instanceof ConcurrencyConflictError
  );

  const state = await store.materialize('accounts');
  assert.equal(state.version, 1);
});

test('rolls back to an active event point and keeps the old events auditable', async () => {
  const store = await createStore();
  await appendUser(store, 'Alice');
  await appendUser(store, 'Bob');
  await appendUser(store, 'Cara');

  const rollback = await store.rollback('users', 2);
  const state = await store.materialize('users');

  assert.equal(rollback.event.version, 4);
  assert.equal(state.version, 4);
  assert.equal(state.state.entities['user-1'].name, 'Bob');
  assert.equal(state.applied, 0);
  assert.ok(state.snapshotUsed, 'rollback should materialize from its snapshot');

  const historical = await store.stateAt('users', { atVersion: 3 });
  assert.equal(historical.state.entities['user-1'].name, 'Cara');

  const all = await store.inspectEvents({
    streamId: 'users',
    includeSystem: true,
  });
  assert.equal(all.count, 4);
});

test('can reset to a previously superseded event point', async () => {
  const store = await createStore();
  await appendUser(store, 'A');
  await appendUser(store, 'B');
  await store.rollback('users', 1);
  await appendUser(store, 'C');
  await store.rollback('users', 2);
  const resetState = await store.materialize('users');
  assert.equal(resetState.state.entities['user-1'].name, 'B');
  assert.equal(resetState.version, 5);

  const oldFork = await store.stateAt('users', { atVersion: 4 });
  assert.equal(oldFork.state.entities['user-1'].name, 'C');

  await appendUser(store, 'D');
  const rebuilt = await store.materialize('users');
  assert.equal(rebuilt.state.entities['user-1'].name, 'D');
  assert.equal(rebuilt.version, 6);
});

test('creates compressed snapshot and restores exact state after replay', async () => {
  const store = await createStore();
  for (let index = 0; index < 10; index += 1) {
    await appendUser(store, `Name ${index}`);
  }

  const snapshot = await store.createSnapshot('users');
  assert.ok(['gzip+json', 'json'].includes(snapshot.codec));
  assert.equal(snapshot.eventVersion, 10);

  const restored = await store.materialize('users');
  assert.equal(restored.state.entities['user-1'].name, 'Name 9');
  assert.equal(restored.snapshotUsed.id, snapshot.id);
  assert.equal(restored.applied, 0);

  const historical = await store.stateAt('users', { atVersion: 5 });
  assert.equal(historical.state.entities['user-1'].name, 'Name 4');
});

test('compaction preserves current and recent snapshots', async () => {
  const store = await createStore();
  for (let index = 0; index < 6; index += 1) {
    await appendUser(store, `Name ${index}`);
    await store.createSnapshot('users');
  }

  const result = await store.compactSnapshots('users', { keep: 2 });
  const snapshots = await store.listSnapshots('users');
  assert.equal(result.deleted.length, 4);
  assert.equal(snapshots.length, 2);

  const state = await store.materialize('users');
  assert.equal(state.state.entities['user-1'].name, 'Name 5');
});

test('quota failure degrades new writes to memory overflow and reads merged state', async () => {
  const primary = new MemoryStorage({ name: 'small', quotaBytes: 1 });
  const fallback = new MemoryStorage({ name: 'overflow' });
  const storage = new OverflowStorage(primary, fallback);
  const store = await createStore({ storage, snapshotEvery: 0 });

  const first = await store.append('limited', 'entity.updated', { name: 'first' }, {
    entityId: 'item',
    snapshot: false,
  });
  assert.equal(first.version, 1);

  const second = await store.append('limited', 'entity.updated', { name: 'second' }, {
    entityId: 'item',
    snapshot: false,
  });
  assert.equal(second.version, 2);

  const state = await store.materialize('limited');
  assert.equal(state.state.entities.item.name, 'second');
  const info = await store.storageInfo();
  assert.deepEqual(info.degradedStreams, ['limited']);
  assert.equal(info.mode, 'degraded');
});

test('batch append is atomic on optimistic conflict', async () => {
  const store = await createStore();
  await appendUser(store, 'Initial');

  await assert.rejects(() => store.appendMany('users', [
    { type: 'entity.updated', entityId: 'user-1', payload: { name: 'X' } },
    { type: 'entity.updated', entityId: 'user-1', payload: { name: 'Y' } },
  ], { expectedVersion: 0, snapshot: false }));

  const state = await store.materialize('users');
  assert.equal(state.version, 1);
  assert.equal(state.state.entities['user-1'].name, 'Initial');
});

test('append succeeds when automatic snapshot quota is exhausted', async () => {
  const primary = new MemoryStorage({ name: 'small', quotaBytes: 1200 });
  const fallback = new MemoryStorage({ name: 'overflow', quotaBytes: 120 });
  const storage = new OverflowStorage(primary, fallback);
  const store = await createStore({ storage, snapshotEvery: 1 });

  const event = await store.append('strict', 'entity.updated', { name: 'kept' }, {
    entityId: 'item',
  });

  assert.equal(event.version, 1);
  const state = await store.materialize('strict');
  assert.equal(state.state.entities.item.name, 'kept');
  assert.equal(state.snapshotUsed, null);
});



test('snapshot and concurrent appends never publish a stale head', async () => {
  const store = await createStore({ snapshotEvery: 0 });
  const operations = [];

  for (let index = 0; index < 30; index += 1) {
    operations.push(
      appendUser(store, `name-${index}`),
      index % 3 === 0
        ? store.createSnapshot('users').catch(() => null)
        : Promise.resolve()
    );
  }

  await Promise.all(operations);
  const [state, stream] = await Promise.all([
    store.materialize('users'),
    store.storage.getStream('users'),
  ]);

  assert.equal(state.version, stream.version);
  assert.equal(state.state.entities['user-1'].name, 'name-29');
});

async function appendUser(store, name) {
  return store.append('users', 'entity.updated', { name }, {
    entityId: 'user-1',
    snapshot: false,
  });
}
