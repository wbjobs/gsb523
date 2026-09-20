import { test } from 'node:test';
import assert from 'node:assert/strict';
import { FakeIndexedDB, IDBKeyRange } from './fake-idb.js';
import { IndexedDbAdapter } from '../src/adapters/idb.js';
import { EventStore } from '../src/engine.js';

globalThis.IDBKeyRange = IDBKeyRange;

function counterReducer(state, event) {
  if (state == null) return { count: 0 };
  if (event.type === 'inc') state.count += (event.payload && event.payload.n) || 1;
  if (event.type === 'dec') state.count -= (event.payload && event.payload.n) || 1;
  if (event.type === 'set') state.count = event.payload.value;
  return state;
}

async function createIdbStore(name) {
  const idb = new FakeIndexedDB();
  const adapter = new IndexedDbAdapter(name, idb);
  await adapter.open();
  const store = new EventStore(adapter, { snapshotEvery: 0 });
  store.registerReducer('counter', counterReducer);
  await store.open();
  return { store, adapter, idb };
}

test('IDB adapter: append/query/state roundtrip', async () => {
  const { store } = await createIdbStore('db1');
  for (let i = 0; i < 20; i++) await store.append('counter', 'inc');
  const rows = await store.query({ stream: 'counter', limit: 100 });
  assert.equal(rows.length, 20);
  assert.equal((await store.getState('counter')).count, 20);
});

test('IDB adapter: bulk append uses putMany and stays ordered', async () => {
  const { store } = await createIdbStore('db2');
  await store.appendStream(
    'counter',
    Array.from({ length: 3000 }, () => ({ type: 'inc' })),
    { expectedVersion: -1, batchSize: 1000 }
  );
  const rows = await store.query({ stream: 'counter', limit: 5000 });
  assert.deepEqual(rows.map((r) => r.seq), Array.from({ length: 3000 }, (_, i) => i + 1));
  assert.deepEqual(
    rows.map((r) => r.globalSeq),
    rows.map((r) => r.globalSeq).sort((a, b) => a - b)
  );
});

test('IDB adapter: rollback + time travel through real indexes', async () => {
  const { store } = await createIdbStore('db3');
  for (let i = 0; i < 6; i++) await store.append('counter', 'inc');
  const point = store.physicalSeq;
  await store.rollback('counter', 2);
  await store.append('counter', 'inc', { n: 10 });
  assert.equal((await store.getState('counter')).count, 12);

  const current = await store.query({ stream: 'counter' });
  assert.deepEqual(current.map((e) => e.seq), [1, 2, 1]);

  const past = await store.query({ stream: 'counter', atGlobalSeq: point });
  assert.equal(past.length, 6);
  assert.equal((await store.getStateAt('counter', point)).count, 6);
});

test('IDB adapter: snapshot written through IDB and used for replay', async () => {
  const idb = new FakeIndexedDB();
  const adapter = new IndexedDbAdapter('db4', idb);
  await adapter.open();
  const store = new EventStore(adapter, { snapshotEvery: 5 });
  store.registerReducer('counter', counterReducer);
  await store.open();
  for (let i = 0; i < 12; i++) await store.append('counter', 'inc');

  // Reopen with a fresh adapter over the same fake DB.
  const adapter2 = new IndexedDbAdapter('db4', idb);
  await adapter2.open();
  const store2 = new EventStore(adapter2, { snapshotEvery: 5 });
  store2.registerReducer('counter', counterReducer);
  await store2.open();
  assert.equal(store2.getHead('counter').seq, 12);
  assert.equal((await store2.getState('counter')).count, 12);
  const verify = await store2.verifyStream('counter');
  assert.equal(verify.ok, true);
});
