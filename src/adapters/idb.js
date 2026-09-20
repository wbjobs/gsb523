/**
 * IndexedDB storage adapter. Same logical contract as MemoryAdapter.
 *
 * Object stores:
 *   events    (keyPath globalSeq)        append-only event records
 *     index [stream, epoch, seq] unique  stream-local lookups / range scans
 *     index stream                       stream-only scans
 *   snapshots (keyPath snapshotId)
 *     index [stream, epoch, baseSeq]
 *   meta      (keyPath key)              singleton keys (heads, counters, quota)
 *
 * globalSeq is allocated by the engine from the meta counter and written
 * explicitly so that batches can be inserted with a tight request loop and
 * records can hash-chain without waiting for each auto-increment key.
 */

const DB_VERSION = 1;
const MAX_SAFE = Number.MAX_SAFE_INTEGER;

function reqAsPromise(request) {
  return new Promise((resolve, reject) => {
    request.onsuccess = () => resolve(request.result);
    request.onerror = () => reject(request.error);
  });
}

function txDone(transaction) {
  return new Promise((resolve, reject) => {
    transaction.oncomplete = () => resolve();
    transaction.onerror = () => reject(transaction.error);
    transaction.onabort = () => reject(transaction.error || new Error('transaction aborted'));
  });
}

export class IndexedDbAdapter {
  constructor(dbName, indexedDbFactory) {
    this.dbName = dbName || 'event-store';
    this._idb = indexedDbFactory || (typeof indexedDB !== 'undefined' ? indexedDB : null);
    this._db = null;
  }

  async open() {
    if (!this._idb) throw new Error('IndexedDB is not available in this environment');
    const request = this._idb.open(this.dbName, DB_VERSION);
    request.onupgradeneeded = () => {
      const db = request.result;
      if (!db.objectStoreNames.contains('events')) {
        const events = db.createObjectStore('events', { keyPath: 'globalSeq' });
        events.createIndex('streamEpochSeq', ['stream', 'epoch', 'seq'], { unique: true });
        events.createIndex('stream', 'stream', { unique: false });
      }
      if (!db.objectStoreNames.contains('snapshots')) {
        const snaps = db.createObjectStore('snapshots', { keyPath: 'snapshotId' });
        snaps.createIndex('streamEpochBaseSeq', ['stream', 'epoch', 'baseSeq'], { unique: false });
      }
      if (!db.objectStoreNames.contains('meta')) {
        db.createObjectStore('meta', { keyPath: 'key' });
      }
    };
    this._db = await reqAsPromise(request);
    return this;
  }

  async close() {
    if (this._db) {
      this._db.close();
      this._db = null;
    }
  }

  async runTransaction(storeNames, mode, fn) {
    if (!this._db) await this.open();
    const transaction = this._db.transaction(storeNames, mode);
    const txView = {
      store: (name) => new IDBStore(transaction.objectStore(name), transaction)
    };
    const result = await fn(txView);
    if (typeof transaction.endSync === 'function') transaction.endSync();
    await txDone(transaction);
    return result;
  }
}

class IDBStore {
  constructor(objectStore, transaction) {
    this._os = objectStore;
    this._tx = transaction;
  }

  put(value) {
    return reqAsPromise(this._os.put(value));
  }

  putMany(values) {
    const pending = [];
    for (const value of values) pending.push(reqAsPromise(this._os.put(value)));
    return Promise.all(pending);
  }

  add(value) {
    return reqAsPromise(this._os.add(value));
  }

  get(key) {
    return reqAsPromise(this._os.get(key));
  }

  deleteSnapshot(snapshotId) {
    return reqAsPromise(this._os.delete(snapshotId));
  }

  scanEvents({ lower = null, upper = null, inclusiveUpper = true } = {}) {
    let range = null;
    if (lower != null && upper != null) {
      range = IDBKeyRange.bound(lower, upper, false, !inclusiveUpper);
    } else if (lower != null) {
      range = IDBKeyRange.lowerBound(lower);
    } else if (upper != null) {
      range = inclusiveUpper ? IDBKeyRange.upperBound(upper) : IDBKeyRange.upperBound(upper, true);
    }
    const request = range ? this._os.openCursor(range) : this._os.openCursor();
    return new IDBCursor(request);
  }

  scanStream(stream, upper) {
    const index = this._os.index('stream');
    if (upper == null) {
      return new IDBCursor(index.openCursor(IDBKeyRange.only(stream)));
    }
    // Index cursors are ordered by [indexKey, primaryKey], so bound the
    // primary key via an explicit value filter in the cursor wrapper.
    return new BoundedIDBCursor(index.openCursor(IDBKeyRange.only(stream)), upper);
  }

  scanStreamRaw(stream, fromSeq, toSeq, epochs) {
    const index = this._os.index('streamEpochSeq');
    const cursors = [];
    for (const epoch of epochs) {
      // Prefix-bounded range over [stream, epoch, *]; the seq window is
      // filtered here so behavior is independent of IDB array-key prefix
      // comparison edge cases.
      const range = IDBKeyRange.bound(
        [stream, epoch, fromSeq],
        [stream, epoch, MAX_SAFE]
      );
      const cursor = new IDBCursor(index.openCursor(range));
      cursors.push(new FilteredCursor(cursor, (value) => value.seq <= toSeq));
    }
    return new MultiCursor(cursors);
  }

  async findStreamRecord(stream, epoch, seq) {
    const index = this._os.index('streamEpochSeq');
    return reqAsPromise(index.get(IDBKeyRange.only([stream, epoch, seq])));
  }

  async findRollbackMarker(stream, epoch) {
    const index = this._os.index('streamEpochSeq');
    const range = IDBKeyRange.bound(
      [stream, epoch, 0],
      [stream, epoch, MAX_SAFE]
    );
    let found = null;
    await new Promise((resolve, reject) => {
      const request = index.openCursor(range);
      request.onsuccess = () => {
        const cursor = request.result;
        if (!cursor) return resolve();
        const value = cursor.value;
        if (value.type && value.type.charAt(0) === '$') {
          found = value;
          return resolve();
        }
        cursor.continue();
      };
      request.onerror = () => reject(request.error);
    });
    return found;
  }

  listSnapshots(stream) {
    const index = this._os.index('streamEpochBaseSeq');
    const range = IDBKeyRange.bound(
      [stream, 0, 0],
      [stream, MAX_SAFE, MAX_SAFE]
    );
    return reqAsPromise(index.getAll(range));
  }

  allSnapshots() {
    return reqAsPromise(this._os.getAll());
  }

  getSnapshot(snapshotId) {
    return reqAsPromise(this._os.get(snapshotId));
  }
}

/**
 * Promise wrapper over an IDB cursor request. Handles the timing gap where
 * onsuccess may fire before the consumer calls next(): values queue up, and
 * continue() is issued exactly once when a consumer is ready for the next
 * row.
 */
class IDBCursor {
  constructor(request) {
    this._request = request;
    this._queue = [];
    this._waiters = [];
    this._done = false;
    this._cursor = null;
    this._primed = false;
    request.onsuccess = () => {
      const cursor = request.result;
      if (!cursor) {
        this._done = true;
        this._cursor = null;
        while (this._waiters.length) this._waiters.shift().resolve(null);
        return;
      }
      this._cursor = cursor;
      this._primed = true;
      if (this._waiters.length) this._advance();
      else this._queue.push(cursor.value);
    };
    request.onerror = () => {
      while (this._waiters.length) this._waiters.shift().reject(request.error);
    };
  }

  _advance() {
    const cursor = this._cursor;
    if (!cursor) return;
    this._cursor = null;
    const waiter = this._waiters.shift();
    // Deliver the value first; the actual continue() is issued once the
    // consumer calls next() again (below). The cursor stays parked here.
    waiter.resolve(cursor.value);
    // Schedule continue for the following next() call by stashing cursor
    this._parked = cursor;
  }

  next() {
    if (this._queue.length) return Promise.resolve(this._queue.shift());
    if (this._done) return Promise.resolve(null);
    if (this._parked) {
      const cursor = this._parked;
      this._parked = null;
      cursor.continue();
    }
    return new Promise((resolve, reject) => {
      this._waiters.push({ resolve, reject });
    });
  }

  [Symbol.asyncIterator]() {
    return {
      next: async () => {
        const value = await this.next();
        return value == null ? { done: true, value: null } : { done: false, value };
      }
    };
  }

  async all() {
    const out = [];
    for (;;) {
      const value = await this.next();
      if (value == null) break;
      out.push(value);
    }
    return out;
  }
}

class BoundedIDBCursor extends IDBCursor {
  constructor(request, upper) {
    super(request);
    this._upper = upper;
    this._boundDone = false;
  }

  next() {
    if (this._boundDone) return Promise.resolve(null);
    return super.next().then((value) => {
      if (value == null) {
        this._boundDone = true;
        return null;
      }
      if (value.globalSeq > this._upper) {
        this._boundDone = true;
        // Consume the remainder so the underlying cursor request settles
        // and the enclosing transaction can commit.
        this._drainRest();
        return null;
      }
      return value;
    });
  }

  async _drainRest() {
    for (;;) {
      const value = await super.next();
      if (value == null) return;
    }
  }
}

class FilteredCursor {
  constructor(cursor, predicate) {
    this._cursor = cursor;
    this._predicate = predicate;
  }

  async next() {
    for (;;) {
      const value = await this._cursor.next();
      if (value == null) return null;
      if (this._predicate(value)) return value;
    }
  }
}

class MultiCursor {
  constructor(cursors) {
    this._cursors = cursors;
    this._buffer = [];
    this._started = false;
  }

  async _drain() {
    if (!this._started) {
      this._started = true;
      for (const cursor of this._cursors) {
        for (;;) {
          const value = await cursor.next();
          if (value == null) break;
          this._buffer.push(value);
        }
      }
      this._buffer.sort((a, b) =>
        a.epoch !== b.epoch ? a.epoch - b.epoch : a.seq - b.seq
      );
      this._index = 0;
    }
  }

  async next() {
    await this._drain();
    if (this._index >= this._buffer.length) return null;
    return this._buffer[this._index++];
  }

  [Symbol.asyncIterator]() {
    return {
      next: async () => {
        const value = await this.next();
        return value == null ? { done: true, value: null } : { done: false, value };
      }
    };
  }

  async all() {
    await this._drain();
    return this._buffer;
  }
}
