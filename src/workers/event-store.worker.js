import { createEventStore } from '../core/create-store.js';
import { createDefaultReducer } from '../core/reducers.js';
import { serializeError } from '../core/errors.js';
import { QueryEngine } from '../core/query-engine.js';

let storePromise;
let queryEngine;

self.onmessage = async (message) => {
  const request = message.data;
  if (!request?.id || request.type !== 'request') return;

  try {
    const result = await handle(request);
    self.postMessage({ id: request.id, type: 'response', result });
  } catch (error) {
    self.postMessage({
      id: request.id,
      type: 'error',
      error: serializeError(error),
    });
  }
};

async function handle(request) {
  if (request.method === 'init') {
    const {
      reducers: customReducers,
      reducerVersions = {},
      ...options
    } = request.options || {};
    const reducers = new Map([['*', createDefaultReducer()]]);

    for (const [streamId, factoryName] of Object.entries(customReducers || {})) {
      if (factoryName === 'default') {
        const reducer = createDefaultReducer();
        reducer.version = reducerVersions[streamId] || 1;
        reducers.set(streamId, reducer);
      }
    }

    const store = await createEventStore({
      snapshotEvery: 1000,
      keepSnapshots: 5,
      ...options,
      reducers,
    });
    storePromise = Promise.resolve(store);
    queryEngine = new QueryEngine(store);
    return { ready: true };
  }

  const store = await getStore();

  if (request.method === 'query') {
    return queryEngine.query(request.args[0]);
  }

  if (request.method === 'configureReducer') {
    store.configureReducer(request.streamId || '*', createDefaultReducer());
    return { configured: true };
  }

  const method = store[request.method];
  if (typeof method !== 'function') {
    throw new Error(`Unsupported method: ${request.method}`);
  }

  return method.apply(store, request.args || []);
}

async function getStore() {
  if (!storePromise) {
    storePromise = createEventStore({});
    storePromise.then((store) => {
      queryEngine = new QueryEngine(store);
    });
  }
  return storePromise;
}
