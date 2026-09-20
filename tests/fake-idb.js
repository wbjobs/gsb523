/**
 * Minimal in-memory IndexedDB implementation covering the API surface used
 * by IndexedDbAdapter: object stores, single/multi-entry key paths,
 * unique indexes, key ranges (only/bound/lowerBound/upperBound),
 * getAll/get/openCursor with continue, and async transactions.
 *
 * Not a general-purpose polyfill; just enough to exercise the real adapter
 * code path in Node.
 */

class FakeKeyRange {
  constructor(lower, upper, lowerOpen, upperOpen) {
    this.lower = lower;
    this.upper = upper;
    this.lowerOpen = !!lowerOpen;
    this.upperOpen = !!upperOpen;
  }

  static only(value) {
    return new FakeKeyRange(value, value, false, false);
  }

  static bound(lower, upper, lowerOpen, upperOpen) {
    return new FakeKeyRange(lower, upper, lowerOpen, upperOpen);
  }

  static lowerBound(lower, open) {
    return new FakeKeyRange(lower, null, open, false);
  }

  static upperBound(upper, open) {
    return new FakeKeyRange(null, upper, false, open);
  }
}

function cmp(a, b) {
  if (a === b) return 0;
  return a < b ? -1 : 1;
}

function cmpKeys(a, b) {
  if (Array.isArray(a) && Array.isArray(b)) {
    for (let i = 0; i < Math.max(a.length, b.length); i++) {
      const c = cmp(a[i], b[i]);
      if (c !== 0) return c;
    }
    return 0;
  }
  return cmp(a, b);
}

function keyInRange(key, range) {
  if (!range) return true;
  if (range.lower != null) {
    const c = cmpKeys(key, range.lower);
    if (c < 0 || (c === 0 && range.lowerOpen)) return false;
  }
  if (range.upper != null) {
    const c = cmpKeys(key, range.upper);
    if (c > 0 || (c === 0 && range.upperOpen)) return false;
  }
  return true;
}

class Request extends Promise {
  constructor(executor) {
    let resolveFn;
    let rejectFn;
    super((resolve, reject) => {
      resolveFn = resolve;
      rejectFn = reject;
      if (executor) executor(resolve, reject);
    });
    this.onsuccess = null;
    this.onerror = null;
    this.result = undefined;
    this.error = null;
    this._resolveFn = resolveFn;
    this._rejectFn = rejectFn;
  }

  succeed(value) {
    this.result = value;
    queueMicrotask(() => {
      this._resolveFn(value);
      if (this.onsuccess) this.onsuccess({ target: this });
    });
  }

  fail(error) {
    this.error = error;
    queueMicrotask(() => {
      this._rejectFn(error);
      if (this.onerror) this.onerror({ target: this });
    });
  }
}

/**
 * Cursor scheduling mirrors real IDB:
 *  - openCursor schedules one onsuccess (async) with the first match
 *  - the next onsuccess arrives ONLY after the consumer calls continue()
 *  - when exhausted, onsuccess fires with null (also only after the
 *    consumer continues past the last row)
 */
class Cursor {
  constructor(rows, range, request, keyGetter) {
    this._rows = rows;
    this._range = range;
    this._keyGetter = keyGetter;
    this._pos = 0;
    this._request = request;
    this._wantNext = true;
    this._finished = false;
    this.done = new Promise((resolve) => {
      this._doneResolve = resolve;
    });
    queueMicrotask(() => this._pump());
  }

  _makeView(value) {
    return {
      value,
      continue: () => {
        this._pos += 1;
        this._wantNext = true;
        queueMicrotask(() => this._pump());
      }
    };
  }

  _pump() {
    if (!this._wantNext || this._finished) return;
    this._wantNext = false;
    while (this._pos < this._rows.length) {
      const value = this._rows[this._pos];
      if (keyInRange(this._keyGetter(value), this._range)) {
        this._request.result = this._makeView(value);
        this._request.onsuccess && this._request.onsuccess({ target: this._request });
        return;
      }
      this._pos += 1;
    }
    this._finished = true;
    this._request.result = null;
    this._request.onsuccess && this._request.onsuccess({ target: this._request });
    this._doneResolve();
  }
}

class ObjectStore {
  constructor(name, keyPath, indexes, tables, transaction) {
    this.name = name;
    this.keyPath = keyPath;
    this._indexDefs = indexes;
    this._tables = tables;
    this._tx = transaction || null;
  }

  _track(promise) {
    if (this._tx) this._tx._track(promise);
    return promise;
  }

  _table() {
    return this._tables[this.name];
  }

  _primaryKey(value) {
    return value[this.keyPath];
  }

  put(value) {
    const request = new Request();
    queueMicrotask(() => {
      const table = this._table();
      const key = this._primaryKey(value);
      table.rows.set(key, structuredClone(value));
      request.succeed(key);
    });
    this._track(request.catch(() => {}));
    return request;
  }

  add(value) {
    const request = new Request();
    queueMicrotask(() => {
      const key = this._primaryKey(value);
      if (this._table().rows.has(key)) {
        const error = new Error('ConstraintError');
        error.name = 'ConstraintError';
        request.fail(error);
        return;
      }
      this._table().rows.set(key, structuredClone(value));
      request.succeed(key);
    });
    this._track(request.catch(() => {}));
    return request;
  }

  get(key) {
    const request = new Request();
    queueMicrotask(() => {
      const value = this._table().rows.get(key);
      request.succeed(value ? structuredClone(value) : undefined);
    });
    this._track(request.catch(() => {}));
    return request;
  }

  getAll(range) {
    const request = new Request();
    queueMicrotask(() => {
      const rows = [...this._table().rows.values()].sort((a, b) =>
        cmpKeys(a[this.keyPath], b[this.keyPath])
      );
      const matched = rows
        .filter((row) => keyInRange(row[this.keyPath], range))
        .map((row) => structuredClone(row));
      request.succeed(matched);
    });
    this._track(request.catch(() => {}));
    return request;
  }

  delete(key) {
    const request = new Request();
    queueMicrotask(() => {
      this._table().rows.delete(key);
      request.succeed(undefined);
    });
    this._track(request.catch(() => {}));
    return request;
  }

  openCursor(range) {
    const request = new Request();
    const rows = [...this._table().rows.values()].sort((a, b) =>
      cmpKeys(a[this.keyPath], b[this.keyPath])
    );
    const cursor = new Cursor(rows, range, request, (row) => row[this.keyPath]);
    this._track(cursor.done);
    return request;
  }

  index(name) {
    return new Index(this, this._indexDefs[name]);
  }
}

class Index {
  constructor(store, def) {
    this._store = store;
    this._def = def;
    this._track = store._track.bind(store);
  }

  _indexKey(row) {
    const path = this._def.keyPath;
    return Array.isArray(path) ? path.map((part) => row[part]) : row[path];
  }

  _sortedRows() {
    return [...this._store._table().rows.values()]
      .map((row) => ({ row, key: this._indexKey(row) }))
      .sort((a, b) => cmpKeys(a.key, b.key))
      .map((x) => x.row);
  }

  get(range) {
    const request = new Request();
    queueMicrotask(() => {
      const found = this._sortedRows().find((row) => keyInRange(this._indexKey(row), range));
      request.succeed(found ? structuredClone(found) : undefined);
    });
    this._track(request.catch(() => {}));
    return request;
  }

  getAll(range) {
    const request = new Request();
    queueMicrotask(() => {
      const matched = this._sortedRows()
        .filter((row) => keyInRange(this._indexKey(row), range))
        .map((row) => structuredClone(row));
      request.succeed(matched);
    });
    this._track(request.catch(() => {}));
    return request;
  }

  openCursor(range) {
    const request = new Request();
    const rows = this._sortedRows();
    const cursor = new Cursor(rows, range, request, (row) => this._indexKey(row));
    this._track(cursor.done);
    return request;
  }
}

class Transaction {
  constructor(db, storeNames, mode) {
    this.db = db;
    this.mode = mode;
    this.oncomplete = null;
    this.onerror = null;
    this.onabort = null;
    this.error = null;
    this._active = true;
    this._pending = 0;
    this._settled = false;
    this._syncEnded = false;
  }

  endSync() {
    this._syncEnded = true;
    this._maybeComplete();
  }

  _track(promise) {
    this._pending += 1;
    const done = () => {
      this._pending -= 1;
      queueMicrotask(() => this._maybeComplete());
    };
    promise.then(done, done);
  }

  _maybeComplete() {
    if (this._settled || this._pending > 0 || !this._syncEnded) return;
    this._settled = true;
    this._active = false;
    queueMicrotask(() => this.oncomplete && this.oncomplete());
  }

  objectStore(name) {
    return new ObjectStore(
      name,
      this.db._defs[name].keyPath,
      this.db._defs[name].indexes,
      this.db._tables,
      this
    );
  }
}

class Database {
  constructor(name) {
    this.name = name;
    this._defs = {};
    this._tables = {};
    this.objectStoreNames = {
      _set: new Set(),
      add(name) { this._set.add(name); },
      contains(name) { return this._set.has(name); }
    };
    this.onversionchange = null;
  }

  createObjectStore(name, options = {}) {
    this._defs[name] = { keyPath: options.keyPath, indexes: {} };
    this._tables[name] = { rows: new Map() };
    this.objectStoreNames.add(name);
    const db = this;
    const store = new ObjectStore(name, options.keyPath, this._defs[name].indexes, this._tables);
    store.createIndex = (indexName, keyPath) => {
      db._defs[name].indexes[indexName] = { keyPath };
    };
    return store;
  }

  transaction(storeNames, mode) {
    return new Transaction(this, storeNames, mode);
  }

  close() {}
}

class OpenRequest {
  constructor() {
    this.result = null;
    this.onsuccess = null;
    this.onerror = null;
    this.onupgradeneeded = null;
  }
}

export class FakeIndexedDB {
  constructor() {
    this._databases = new Map();
  }

  open(name) {
    const request = new OpenRequest();
    const exists = this._databases.has(name);
    const db = exists ? this._databases.get(name) : new Database(name);
    request.result = db;
    queueMicrotask(async () => {
      if (!exists) {
        this._databases.set(name, db);
        if (request.onupgradeneeded) request.onupgradeneeded({ target: request });
      }
      if (request.onsuccess) request.onsuccess({ target: request });
    });
    return request;
  }

  deleteDatabase(name) {
    const request = new OpenRequest();
    queueMicrotask(() => {
      this._databases.delete(name);
      if (request.onsuccess) request.onsuccess({ target: request });
    });
    return request;
  }
}

export { FakeKeyRange as IDBKeyRange };
