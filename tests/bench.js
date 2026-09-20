/* Standalone benchmark: node tests/bench.js */
import { createStore } from './helpers.js';

const TOTAL = Number(process.argv[2] || 100_000);
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

let t = Date.now();
await store.appendStream('bench', events, { expectedVersion: -1, batchSize: 5000 });
console.log(`append ${TOTAL}: ${Date.now() - t}ms`);

t = Date.now();
const all = await store.query({ stream: 'bench', limit: TOTAL });
console.log(`query all ${all.length}: ${Date.now() - t}ms`);

t = Date.now();
const special = await store.query({ stream: 'bench', type: 'special', limit: TOTAL });
console.log(`query filtered ${special.length}: ${Date.now() - t}ms`);

const mid = Math.floor(store.physicalSeq / 2);
t = Date.now();
const past = await store.query({ stream: 'bench', atGlobalSeq: mid, limit: TOTAL });
console.log(`time travel @${mid} ${past.length}: ${Date.now() - t}ms`);

t = Date.now();
const state = await store.getState('bench');
console.log(`replay state n=${state.n} sum=${state.sum}: ${Date.now() - t}ms`);
