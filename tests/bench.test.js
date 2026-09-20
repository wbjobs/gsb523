import { test } from 'node:test';
import assert from 'node:assert/strict';
import { createStore } from './helpers.js';

const TOTAL = 100_000;

test(`bulk load ${TOTAL} events and full-scan query stays under 1s`, async () => {
  const { store } = await createStore({
    snapshotEvery: 0,
    extraReducers: {
      bench: (state, event) => {
        if (state == null) return { sum: 0, n: 0 };
        state.sum += event.payload.value;
        state.n += 1;
        return state;
      }
    }
  });

  const events = Array.from({ length: TOTAL }, (_, i) => ({
    type: i % 10 === 0 ? 'special' : 'tick',
    payload: { value: i % 7, idx: i }
  }));

  const writeStart = Date.now();
  await store.appendStream('bench', events, { expectedVersion: -1, batchSize: 5000 });
  const writeMs = Date.now() - writeStart;

  assert.equal(store.getHead('bench').seq, TOTAL);

  const queryStart = Date.now();
  const all = await store.query({ stream: 'bench', limit: TOTAL });
  const queryMs = Date.now() - queryStart;
  assert.equal(all.length, TOTAL);
  assert.ok(queryMs < 1000, `full scan took ${queryMs}ms (limit 1000ms)`);

  const filteredStart = Date.now();
  const special = await store.query({
    stream: 'bench',
    type: 'special',
    limit: TOTAL
  });
  const filteredMs = Date.now() - filteredStart;
  assert.equal(special.length, TOTAL / 10);
  assert.ok(filteredMs < 1000, `filtered query took ${filteredMs}ms`);

  const predStart = Date.now();
  const hot = await store.query({
    stream: 'bench',
    filter: (event) => event.payload.value === 6 && event.payload.idx > 90_000,
    limit: TOTAL
  });
  const predMs = Date.now() - predStart;
  assert.ok(hot.length > 0);
  assert.ok(predMs < 1000, `predicate query took ${predMs}ms`);

  // Time travel at 50% point.
  const midGlobal = Math.floor(store.physicalSeq / 2);
  const ttStart = Date.now();
  const mid = await store.query({ stream: 'bench', atGlobalSeq: midGlobal, limit: TOTAL });
  const ttMs = Date.now() - ttStart;
  assert.equal(mid.length, midGlobal);
  assert.ok(ttMs < 1000, `time-travel query took ${ttMs}ms`);

  console.log(`BENCH write=${writeMs}ms query=${queryMs}ms filtered=${filteredMs}ms predicate=${predMs}ms tt=${ttMs}ms`);
});

test(`state materialization of ${TOTAL} events completes (snapshotless replay)`, async () => {
  const { store } = await createStore({
    snapshotEvery: 0,
    extraReducers: {
      bench2: (state, event) => {
        if (state == null) return { sum: 0 };
        state.sum += event.payload.value;
        return state;
      }
    }
  });
  await store.appendStream(
    'bench2',
    Array.from({ length: TOTAL }, (_, i) => ({ type: 'tick', payload: { value: 1 } })),
    { batchSize: 10000 }
  );
  const start = Date.now();
  const state = await store.getState('bench2');
  const ms = Date.now() - start;
  assert.equal(state.sum, TOTAL);
  assert.ok(ms < 2000, `full replay took ${ms}ms`);
});
