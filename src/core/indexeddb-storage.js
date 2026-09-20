import { ConcurrencyConflictError, isQuotaError, QuotaExhaustedError } from './errors.js';

const DB_VERSION = 1;
const EVENTS = 'events';
const SNAPSHOTS = 'snapshots';
const STREAMS = 'streams';
const META = 'meta';
const MAX_VERSION = Number.MAX_SAFE_INTEGER;

export class IndexedDbStorage {
  constructor(dbName = 'event-sourcing-store') {
    this.dbName = dbName;
    this.dbPromise = null;
  }

  async open() {
    if (this.dbPromise) return this.dbPromise;

    this.dbPromise = new Promise((resolve, reject) => {
      if (typeof indexedDB === 'undefined') {
        reject(new Error('IndexedDB is not available'));
        return;
      }

      const request = indexedDB.open(this.dbName, DB_VERSION);
      request.onupgradeneeded = () => upgrade(request.result);
      request.onsuccess = () => resolve(request.result);
      request.onerror = () => reject(request.error);
    });

    return this.dbPromise;
  }

  async close() {
    if (!this.dbPromise) return;
    const db = await this.dbPromise;
    db.close();
    this.dbPromise = null;
  }

  async getMeta(key, fallback = null) {
    const result = await this.read(META, (store) => store.get(key));
    return result === undefined ? fallback : result;
  }

  async setMeta(key, value) {
    await this.write([META], (store) => store.put({ key, value }), 'readwrite');
  }

  async getStream(streamId) {
    await this.open();
    const stream = await this.read(STREAMS, (store) => store.get(streamId));
    return stream || defaultStream(streamId);
  }

  async getEventsByIds(ids) {
    if (!ids.length) return [];
    const events = await this.readMany(EVENTS, ids);
    return events.filter(Boolean).sort(byVersion);
  }

  async getEventsByStream(streamId, filters = {}) {
    const fromVersion = filters.fromVersion ?? 1;
    const toVersion = filters.toVersion ?? MAX_VERSION;
    const range = IDBKeyRange.bound(
      [streamId, fromVersion],
      [streamId, toVersion]
    );

    return this.readIndex(EVENTS, 'streamVersion', range, {
      ...filters,
      includeSystem: filters.includeSystem ?? true,
    });
  }

  async getEventsRange(streamId, fromVersion, toVersion) {
    const range = IDBKeyRange.bound(
      [streamId, fromVersion],
      [streamId, toVersion]
    );
    return this.readIndex(EVENTS, 'streamVersion', range);
  }

  async queryEvents(filters = {}) {
    await this.open();
    const storageFilters = {
      ...filters,
      includeSystem: true,
      types: undefined,
      entityId: undefined,
      from: undefined,
      to: undefined,
    };
    const items = await this.collectEvents(storageFilters);
    const filtered = items.filter((event) => matchesEvent(event, filters));
    const count = filtered.length;
    return {
      items: applyWindow(filtered, filters),
      count,
      plan: buildPlan(filters),
    };
  }

  async commitEvents(events, nextStreams) {
    if (!events.length && !nextStreams.length) return;
    await this.open();

    try {
      await this.transaction(
        [EVENTS, STREAMS],
        'readwrite',
        async ({ stores }) => {
          const streamStore = stores[STREAMS];
          const eventStore = stores[EVENTS];

          const grouped = new Map();
          for (const event of events) {
            const list = grouped.get(event.streamId) || [];
            list.push(event);
            grouped.set(event.streamId, list);
          }

          for (const [streamId, streamEvents] of grouped) {
            streamEvents.sort(byVersion);
            const current = await reqToPromise(streamStore.get(streamId));
            const currentVersion = current?.version || 0;
            let expectedVersion = currentVersion;
            for (const event of streamEvents) {
              if (event.expectedVersion !== expectedVersion) {
                throw new ConcurrencyConflictError(
                  `IndexedDB stream ${streamId} changed`,
                  {
                    streamId,
                    expectedVersion: event.expectedVersion,
                    currentVersion,
                  }
                );
              }
              expectedVersion = event.version;
            }
          }

          for (const event of events) eventStore.put(event);
          for (const stream of nextStreams) streamStore.put(stream);
        }
      );
    } catch (error) {
      throw normalizeQuota(error);
    }
  }

  async commitSnapshot(snapshot, nextStream = null) {
    await this.open();
    if (nextStream) {
      const current = await this.read(STREAMS, (store) =>
        store.get(nextStream.id)
      );
      if (current && current.version !== nextStream.stateHeadEventVersion) {
        throw new ConcurrencyConflictError(
          `Snapshot for ${nextStream.id} is stale`,
          {
            streamId: nextStream.id,
            currentVersion: current.version,
            snapshotVersion: nextStream.stateHeadEventVersion,
          }
        );
      }
    }

    try {
      await this.transaction(
        [SNAPSHOTS, STREAMS],
        'readwrite',
        async ({ stores }) => {
          const snapshotStore = stores[SNAPSHOTS];
          const streamStore = stores[STREAMS];

          if (nextStream) {
            streamStore.put(nextStream);
          }

          snapshotStore.put(snapshot);
        }
      );
    } catch (error) {
      throw normalizeQuota(error);
    }
  }

  async getSnapshotCandidates(streamId) {
    const range = IDBKeyRange.bound(
      [streamId, 0],
      [streamId, MAX_VERSION]
    );
    const snapshots = await this.readIndex(
      SNAPSHOTS,
      'streamEventVersion',
      range
    );
    return snapshots.sort(
      (left, right) => right.eventVersion - left.eventVersion
    );
  }

  async deleteSnapshot(id) {
    await this.write([SNAPSHOTS], (store) => store.delete(id), 'readwrite');
  }

  async listSnapshots(streamId = null) {
    await this.open();
    let snapshots;
    if (streamId) {
      snapshots = await this.readIndex(
        SNAPSHOTS,
        'streamIdOnly',
        IDBKeyRange.only(streamId)
      );
    } else {
      snapshots = await this.read(SNAPSHOTS, (store) => store.getAll());
    }

    return snapshots
      .map(({ stateBytes, ...metadata }) => ({
        ...metadata,
        stateBytes: stateBytes?.byteLength || 0,
      }))
      .sort((left, right) => right.createdAt - left.createdAt);
  }

  async storageInfo() {
    await this.open();
    const [eventCount, streamCount, snapshotCount] = await Promise.all([
      this.count(EVENTS),
      this.count(STREAMS),
      this.count(SNAPSHOTS),
    ]);
    const estimate =
      typeof navigator !== 'undefined' && navigator.storage?.estimate
        ? await navigator.storage.estimate()
        : {};

    return {
      backend: 'indexeddb',
      mode: 'normal',
      usedBytes: estimate.usage ?? null,
      quotaBytes: estimate.quota ?? null,
      eventCount,
      streamCount,
      snapshotCount,
      degradedStreams: [],
    };
  }

  async collectEvents(storageFilters) {
    const filters = storageFilters;
    if (filters.streamId && filters.entityId) {
      return this.readIndex(
        EVENTS,
        'streamEntityVersion',
        IDBKeyRange.bound(
          [filters.streamId, filters.entityId, 0],
          [filters.streamId, filters.entityId, MAX_VERSION]
        ),
        storageFilters
      );
    }

    if (filters.streamId && filters.types?.length === 1) {
      return this.readIndex(
        EVENTS,
        'streamTypeVersion',
        IDBKeyRange.bound(
          [filters.streamId, filters.types[0], 0],
          [filters.streamId, filters.types[0], MAX_VERSION]
        ),
        storageFilters
      );
    }

    if (filters.streamId) {
      const from = Number.isFinite(filters.fromVersion)
        ? filters.fromVersion
        : 1;
      const to = Number.isFinite(filters.toVersion)
        ? filters.toVersion
        : MAX_VERSION;
      return this.readIndex(
        EVENTS,
        'streamVersion',
        IDBKeyRange.bound([filters.streamId, from], [filters.streamId, to]),
        storageFilters
      );
    }

    return this.read(EVENTS, (store) => store.getAll());
  }

  transaction(storeNames, mode, callback) {
    return new Promise((resolve, reject) => {
      this.open().then((db) => {
        const transaction = db.transaction(storeNames, mode);
        const stores = Object.fromEntries(
          storeNames.map((name) => [name, transaction.objectStore(name)])
        );
        let result;

        transaction.oncomplete = () => resolve(result);
        transaction.onerror = () => reject(transaction.error);
        transaction.onabort = () => reject(transaction.error);

        Promise.resolve(callback({ stores, transaction }))
          .then((callbackResult) => {
            result = callbackResult;
          })
          .catch((error) => {
            reject(error);
            transaction.abort();
          });
      }, reject);
    });
  }

  async read(storeName, operation, mode = 'readonly') {
    return this.transaction([storeName], mode, ({ stores }) =>
      reqToPromise(operation(stores[storeName]))
    );
  }

  async readMany(storeName, keys) {
    return this.transaction([storeName], 'readonly', async ({ stores }) => {
      const values = [];
      for (const key of keys) {
        values.push(await reqToPromise(stores[storeName].get(key)));
      }
      return values;
    });
  }

  write(storeNames, operation, mode = 'readwrite') {
    return this.transaction(storeNames, mode, ({ stores }) => {
      if (storeNames.length === 1) {
        return reqToPromise(operation(stores[storeNames[0]]));
      }
      return operation(stores);
    });
  }

  readIndex(storeName, indexName, range = null, filters = {}) {
    return this.transaction([storeName], 'readonly', ({ stores }) => {
      return new Promise((resolve, reject) => {
        const items = [];
        const index = stores[storeName].index(indexName);
        const request = index.openCursor(range, 'next');
        let skipped = 0;
        const offset = filters.offset ?? 0;
        const limit = filters.limit ?? Infinity;

        request.onsuccess = () => {
          const cursor = request.result;
          if (!cursor || items.length >= limit) {
            resolve(items);
            return;
          }

          const value = cursor.value;
          if (skipped < offset) {
            skipped += 1;
          } else {
            items.push(filters.keysOnly ? value.id : value);
          }
          cursor.continue();
        };
        request.onerror = () => reject(request.error);
      });
    });
  }

  count(storeName) {
    return this.read(storeName, (store) => store.count());
  }
}

function upgrade(db) {
  if (!db.objectStoreNames.contains(EVENTS)) {
    const store = db.createObjectStore(EVENTS, { keyPath: 'id' });
    store.createIndex('streamVersion', ['streamId', 'version'], { unique: true });
    store.createIndex('streamTypeVersion', ['streamId', 'type', 'version']);
    store.createIndex('streamEntityVersion', ['streamId', 'entityId', 'version']);
    store.createIndex('streamTimestamp', ['streamId', 'timestamp', 'version']);
  }

  if (!db.objectStoreNames.contains(SNAPSHOTS)) {
    const store = db.createObjectStore(SNAPSHOTS, { keyPath: 'id' });
    store.createIndex('streamEventVersion', ['streamId', 'eventVersion']);
    store.createIndex('streamIdOnly', 'streamId');
  }

  if (!db.objectStoreNames.contains(STREAMS)) {
    db.createObjectStore(STREAMS, { keyPath: 'id' });
  }

  if (!db.objectStoreNames.contains(META)) {
    db.createObjectStore(META, { keyPath: 'key' });
  }
}

function reqToPromise(request) {
  return new Promise((resolve, reject) => {
    request.onsuccess = () => resolve(request.result);
    request.onerror = () => reject(request.error);
  });
}

function matchesEvent(event, filters) {
  if (filters.streamId && event.streamId !== filters.streamId) return false;
  if (!filters.includeSystem && event.type.startsWith('__')) return false;
  if (filters.types?.length && !filters.types.includes(event.type)) return false;
  if (filters.entityId && event.entityId !== filters.entityId) return false;
  if (Number.isFinite(filters.fromVersion) && event.version < filters.fromVersion) return false;
  if (Number.isFinite(filters.toVersion) && event.version > filters.toVersion) return false;
  if (filters.from && event.timestamp < filters.from) return false;
  if (filters.to && event.timestamp > filters.to) return false;
  return true;
}

function applyWindow(items, filters) {
  const offset = filters.offset ?? 0;
  if (filters.limit == null) return items.slice(offset);
  return items.slice(offset, offset + filters.limit);
}

function buildPlan(filters) {
  if (filters.streamId && filters.entityId) {
    return { index: 'streamEntityVersion', postFilter: true };
  }
  if (filters.streamId && filters.types?.length === 1) {
    return { index: 'streamTypeVersion', postFilter: true };
  }
  if (filters.streamId) return { index: 'streamVersion', postFilter: true };
  return { index: 'fullScan', postFilter: true };
}

function normalizeQuota(error) {
  if (isQuotaError(error)) {
    return new QuotaExhaustedError(error.message || 'IndexedDB quota exceeded', {
      cause: error.name,
    });
  }
  return error;
}

function defaultStream(streamId) {
  return {
    id: streamId,
    version: 0,
    headId: null,
    headHash: null,
    rewrites: [],
    stateHeadId: null,
    stateHeadEventVersion: null,
    createdAt: 0,
    updatedAt: 0,
  };
}

function byVersion(left, right) {
  const streamOrder = left.streamId.localeCompare(right.streamId);
  return streamOrder || left.version - right.version;
}
