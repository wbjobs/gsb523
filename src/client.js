import { QuotaExceededError, VersionConflictError, ReadOnlyError, StoreError } from './errors.js';

/**
 * Promise-based client for the store worker. Also works in Node tests when
 * constructed with a custom transport:
 *
 *   new EventStoreClient({ transport: (msg) => server.dispatch(msg) })
 */
export class EventStoreClient {
  constructor(options = {}) {
    this._nextId = 1;
    this._pending = new Map();
    this._transport = options.transport || null;
    this._worker = null;
    if (!this._transport && typeof Worker !== 'undefined' && options.workerUrl) {
      this._worker = new Worker(options.workerUrl, { type: 'module' });
      this._worker.onmessage = (event) => this._onMessage(event.data);
    } else if (!this._transport && typeof Worker !== 'undefined') {
      this._worker = new Worker(new URL('./worker.js', import.meta.url), { type: 'module' });
      this._worker.onmessage = (event) => this._onMessage(event.data);
    }
  }

  static async create(options = {}) {
    const client = new EventStoreClient(options);
    await client.init(options);
    return client;
  }

  _onMessage(data) {
    if (!data || data.id == null) return;
    const pending = this._pending.get(data.id);
    if (!pending) return;
    this._pending.delete(data.id);
    if (data.error) pending.reject(hydrateError(data.error));
    else pending.resolve(data.result);
  }

  _call(method, args) {
    if (this._transport) {
      return Promise.resolve()
        .then(() => this._transport({ id: this._nextId++, method, args }))
        .then((data) => {
          if (data.error) throw hydrateError(data.error);
          return data.result;
        });
    }
    if (!this._worker) return Promise.reject(new Error('no worker transport available'));
    const id = this._nextId++;
    return new Promise((resolve, reject) => {
      this._pending.set(id, { resolve, reject });
      this._worker.postMessage({ id, method, args });
    });
  }

  init(options) {
    return this._call('init', [
      {
        adapter: options.adapter,
        dbName: options.dbName,
        snapshotEvery: options.snapshotEvery,
        compressSnapshots: options.compressSnapshots,
        quota: options.quota,
        reducers: options.reducers
      }
    ]);
  }

  registerReducer(stream, source) {
    return this._call('registerReducer', [stream, source]);
  }

  append(stream, type, payload, expectedVersion) {
    return this._call('append', [stream, type, payload, expectedVersion]);
  }

  appendStream(stream, events, options = {}) {
    return this._call('appendStream', [stream, events, options]);
  }

  rollback(stream, targetSeq, expectedVersion) {
    return this._call('rollback', [stream, targetSeq, expectedVersion]);
  }

  snapshot(stream) {
    return this._call('snapshot', [stream]);
  }

  query(options) {
    return this._call('query', [options]);
  }

  getState(stream) {
    return this._call('getState', [stream]);
  }

  getStateAt(stream, globalSeq) {
    return this._call('getStateAt', [stream, globalSeq]);
  }

  getHead(stream) {
    return this._call('getHead', [stream]);
  }

  listStreams() {
    return this._call('listStreams', []);
  }

  verifyStream(stream) {
    return this._call('verifyStream', [stream]);
  }

  estimateQuota() {
    return this._call('estimateQuota', []);
  }

  setQuotaMode(mode) {
    return this._call('setQuotaMode', [mode]);
  }

  compact(stream) {
    return this._call('compact', [stream]);
  }

  get physicalSeq() {
    return this._call('get physicalSeq', []);
  }

  terminate() {
    if (this._worker) this._worker.terminate();
    this._pending.clear();
  }
}

function hydrateError(error) {
  const map = {
    VERSION_CONFLICT: VersionConflictError,
    QUOTA_EXCEEDED: QuotaExceededError,
    READ_ONLY: ReadOnlyError
  };
  const Klass = map[error.code] || StoreError;
  const err = new Klass(error.message, error.detail);
  err.name = error.name;
  err.code = error.code;
  return err;
}
