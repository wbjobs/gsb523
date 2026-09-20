export class EventStoreClient {
  constructor(workerUrl = new URL('../workers/event-store.worker.js', import.meta.url), options = {}) {
    this.worker = options.worker || new Worker(workerUrl, { type: 'module' });
    this.nextId = 1;
    this.pending = new Map();

    this.worker.onmessage = (message) => {
      const response = message.data;
      if (!response?.id || !this.pending.has(response.id)) return;
      const { resolve, reject } = this.pending.get(response.id);
      this.pending.delete(response.id);

      if (response.type === 'error') {
        const error = new Error(response.error.message);
        Object.assign(error, response.error);
        reject(error);
      } else {
        resolve(response.result);
      }
    };

    this.worker.onerror = (error) => {
      for (const { reject } of this.pending.values()) reject(error);
      this.pending.clear();
    };
  }

  async init(options = {}) {
    return this.call('init', [], options);
  }

  query(request) {
    return this.call('query', [request]);
  }

  append(...args) {
    return this.call('append', args);
  }

  appendMany(...args) {
    return this.call('appendMany', args);
  }

  rollback(...args) {
    return this.call('rollback', args);
  }

  materialize(...args) {
    return this.call('materialize', args);
  }

  stateAt(...args) {
    return this.call('stateAt', args);
  }

  queryEvents(...args) {
    return this.call('queryEvents', args);
  }

  inspectEvents(...args) {
    return this.call('inspectEvents', args);
  }

  createSnapshot(...args) {
    return this.call('createSnapshot', args);
  }

  listSnapshots(...args) {
    return this.call('listSnapshots', args);
  }

  compactSnapshots(...args) {
    return this.call('compactSnapshots', args);
  }

  storageInfo(...args) {
    return this.call('storageInfo', args);
  }

  call(method, args = [], options = undefined) {
    const id = `${this.nextId++}`;
    return new Promise((resolve, reject) => {
      this.pending.set(id, { resolve, reject });
      this.worker.postMessage({
        id,
        type: 'request',
        method,
        args,
        options,
      });
    });
  }

  terminate() {
    this.worker.terminate();
    this.pending.clear();
  }
}
