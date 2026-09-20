import { EventStore } from './engine.js';
import { MemoryAdapter } from './adapters/memory.js';
import { IndexedDbAdapter } from './adapters/idb.js';
import { serializeError } from './protocol.js';

/**
 * Store facade living inside the worker. All writes are serialized by the
 * engine mutex; reads run on snapshot-consistent IDB transactions, so a
 * reader can never observe a half-written batch (no dirty reads).
 */
export class EventStoreServer {
  constructor() {
    this.store = null;
  }

  async init(options = {}) {
    const adapter = options.adapter === 'memory'
      ? new MemoryAdapter()
      : new IndexedDbAdapter(options.dbName || 'event-store');
    this.store = new EventStore(adapter, {
      snapshotEvery: options.snapshotEvery ?? 1000,
      compressSnapshots: options.compressSnapshots !== false,
      quota: options.quota || {},
      clock: options.now ? () => options.now() : undefined
    });
    await this.store.open();
    if (options.reducers) {
      for (const [stream, source] of Object.entries(options.reducers)) {
        this.store.registerReducerSource(stream, source);
      }
    }
    return { streams: this.store.listStreams(), physicalSeq: this.store.physicalSeq };
  }

  _ready() {
    if (!this.store) throw new Error('store not initialized; call init first');
    return this.store;
  }

  async handle(method, args = []) {
    if (method === 'init') return this.init(args[0] || {});
    const store = this._ready();
    switch (method) {
      case 'registerReducer':
        store.registerReducerSource(args[0], args[1]);
        return true;
      case 'append':
        return store.append(args[0], args[1], args[2], args[3]);
      case 'appendStream':
        return store.appendStream(args[0], args[1], args[2] || {});
      case 'rollback':
        return store.rollback(args[0], args[1], args[2]);
      case 'snapshot':
        return store.snapshot(args[0]);
      case 'query':
        return store.query(args[0] || {});
      case 'getState':
        return store.getState(args[0]);
      case 'getStateAt':
        return store.getStateAt(args[0], args[1]);
      case 'getHead':
        return store.getHead(args[0]);
      case 'listStreams':
        return store.listStreams();
      case 'verifyStream':
        return store.verifyStream(args[0]);
      case 'estimateQuota':
        return store.quota.estimate();
      case 'setQuotaMode':
        if (args[0] === 'readonly') store.quota.enterReadOnly();
        if (args[0] === 'normal') store.quota.resume();
        return store.quota.mode;
      case 'compact':
        return store.quota.compactSnapshots(args[0] || null);
      case 'get physicalSeq':
        return store.physicalSeq;
      default:
        throw new Error(`unknown method ${method}`);
    }
  }
}

/** Attach RPC handling to a Worker-like global (self/DedicatedWorkerGlobalScope). */
export function attachWorker(globalScope, server) {
  const backend = server || new EventStoreServer();
  globalScope.onmessage = async (event) => {
    const data = event.data;
    if (!data || data.id == null) return;
    try {
      const result = await backend.handle(data.method, data.args);
      globalScope.postMessage({ id: data.id, result });
    } catch (error) {
      globalScope.postMessage({ id: data.id, error: serializeError(error) });
    }
  };
  return backend;
}
