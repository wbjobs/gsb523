import { EventStore } from '../src/engine.js';
import { MemoryAdapter } from '../src/adapters/memory.js';
import { EventStoreClient } from '../src/client.js';
import { EventStoreServer } from '../src/worker-server.js';

export const counterReducerSource = `(state, event) => {
  if (state == null) state = { count: 0 };
  if (event.type === 'inc') state.count += (event.payload && event.payload.n) || 1;
  if (event.type === 'dec') state.count -= (event.payload && event.payload.n) || 1;
  if (event.type === 'set') state.count = event.payload.value;
  return state;
}`;

export function counterReducer(state, event) {
  if (state == null) return { count: 0 };
  if (event.type === 'inc') state.count += (event.payload && event.payload.n) || 1;
  if (event.type === 'dec') state.count -= (event.payload && event.payload.n) || 1;
  if (event.type === 'set') state.count = event.payload.value;
  return state;
}

export async function createStore(options = {}) {
  const adapter = new MemoryAdapter();
  const store = new EventStore(adapter, {
    snapshotEvery: options.snapshotEvery ?? 1000,
    compressSnapshots: options.compressSnapshots !== false,
    quota: options.quota
  });
  if (options.reducer !== false) {
    store.registerReducer('counter', counterReducer);
    if (options.extraReducers) {
      for (const [name, fn] of Object.entries(options.extraReducers)) store.registerReducer(name, fn);
    }
  }
  await store.open();
  return { store, adapter };
}

/** Client wired directly to a server in-process (mimics the Worker RPC hop). */
export async function createClient(options = {}) {
  const server = new EventStoreServer();
  const transport = async (message) => {
    try {
      const result = await server.handle(message.method, message.args);
      return { id: message.id, result };
    } catch (error) {
      const { serializeError } = await import('../src/protocol.js');
      return { id: message.id, error: serializeError(error) };
    }
  };
  const client = new EventStoreClient({ transport });
  await client.init({
    adapter: 'memory',
    snapshotEvery: options.snapshotEvery ?? 1000,
    compressSnapshots: options.compressSnapshots,
    quota: options.quota,
    reducers: { counter: counterReducerSource, ...(options.reducers || {}) }
  });
  return { client, server };
}
