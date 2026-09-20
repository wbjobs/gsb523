/**
 * In-memory storage adapter implementing the same contract as the
 * IndexedDB adapter. Used for tests and as executable documentation.
 */

function compareEvents(a, b) {
  return a.globalSeq - b.globalSeq;
}

class EventTable {
  constructor() {
    this._map = new Map();
    this._sorted = [];
    this._dirty = false;
  }

  clone() {
    const copy = new EventTable();
    copy._map = new Map(this._map);
    copy._dirty = true;
    return copy;
  }

  put(record) {
    this._map.set(record.globalSeq, record);
    this._dirty = true;
  }

  all() {
    if (this._dirty || this._sorted.length !== this._map.size) {
      this._sorted = [...this._map.values()].sort(compareEvents);
      this._dirty = false;
    }
    return this._sorted;
  }
}

export class MemoryAdapter {
  constructor() {
    this._events = new EventTable();
    this._snapshots = [];
    this._meta = new Map();
    this._seq = 0;
    this._injectedFailure = null;
  }

  async open() {
    return this;
  }

  async close() {}

  injectFailure(code) {
    this._injectedFailure = code;
  }

  async runTransaction(storeNames, mode, fn) {
    if (this._injectedFailure) {
      const err = new Error('injected failure');
      err.name = this._injectedFailure;
      throw err;
    }
    const snapshot = {
      events: mode === 'readwrite' ? this._events.clone() : this._events,
      snapshots: this._snapshots.slice(),
      meta: new Map(this._meta),
      seq: this._seq
    };
    const tx = new MemoryTx(snapshot, mode);
    const result = await fn(tx);
    if (mode === 'readwrite') {
      this._events = snapshot.events;
      this._snapshots = snapshot.snapshots;
      this._meta = snapshot.meta;
      this._seq = snapshot.seq;
    }
    return result;
  }
}

class MemoryTx {
  constructor(snap, mode) {
    this._snap = snap;
    this.readOnly = mode === 'readonly';
  }

  store(name) {
    return new MemoryStore(name, this._snap, this.readOnly);
  }
}

class MemoryStore {
  constructor(name, snap, readOnly) {
    this.name = name;
    this._snap = snap;
    this.readOnly = readOnly;
  }

  _assertWritable() {
    if (this.readOnly) throw new Error('transaction is read-only');
  }

  async put(value) {
    this._assertWritable();
    if (this.name === 'events') {
      this._snap.seq = Math.max(this._snap.seq, value.globalSeq);
      this._snap.events.put(value);
      return value.globalSeq;
    }
    if (this.name === 'snapshots') {
      const idx = this._snap.snapshots.findIndex((s) => s.snapshotId === value.snapshotId);
      if (idx >= 0) this._snap.snapshots[idx] = value;
      else this._snap.snapshots.push(value);
      return value.snapshotId;
    }
    if (this.name === 'meta') {
      this._snap.meta.set(value.key, value);
      return value.key;
    }
    throw new Error(`unknown store ${this.name}`);
  }

  async putMany(values) {
    this._assertWritable();
    if (this.name !== 'events') {
      for (const value of values) await this.put(value);
      return;
    }
    for (const value of values) {
      this._snap.seq = Math.max(this._snap.seq, value.globalSeq);
      this._snap.events.put(value);
    }
  }

  async add(value) {
    this._assertWritable();
    if (this.name === 'meta') {
      if (this._snap.meta.has(value.key)) {
        const err = new Error('ConstraintError');
        err.name = 'ConstraintError';
        throw err;
      }
      this._snap.meta.set(value.key, value);
      return value.key;
    }
    return this.put(value);
  }

  async get(key) {
    if (this.name === 'meta') return this._snap.meta.get(key) || null;
    throw new Error('get unsupported');
  }

  scanEvents({ lower = null, upper = null, inclusiveUpper = true } = {}) {
    let rows = this._snap.events.all();
    if (lower != null) {
      rows = rows.filter((e) => e.globalSeq >= lower);
    }
    if (upper != null) {
      rows = rows.filter((e) => (inclusiveUpper ? e.globalSeq <= upper : e.globalSeq < upper));
    }
    return makeCursor(rows.map((r) => ({ ...r })));
  }

  scanStream(stream, upper = null) {
    let rows = this._snap.events.all().filter((e) => e.stream === stream);
    if (upper != null) rows = rows.filter((e) => e.globalSeq <= upper);
    return makeCursor(rows.map((r) => ({ ...r })));
  }

  scanStreamRaw(stream, fromSeq, toSeq, epochs) {
    const epochSet = new Set(epochs);
    const rows = this._snap.events
      .all()
      .filter(
        (e) =>
          e.stream === stream &&
          epochSet.has(e.epoch) &&
          e.seq >= fromSeq &&
          e.seq <= toSeq
      )
      .sort((a, b) => (a.epoch !== b.epoch ? a.epoch - b.epoch : a.seq - b.seq));
    return makeCursor(rows.map((r) => ({ ...r })));
  }

  async findStreamRecord(stream, epoch, seq) {
    const found = this._snap.events
      .all()
      .find((e) => e.stream === stream && e.epoch === epoch && e.seq === seq);
    return found ? { ...found } : null;
  }

  async findRollbackMarker(stream, epoch) {
    const found = this._snap.events
      .all()
      .find((e) => e.stream === stream && e.epoch === epoch - 1 && e.type === '$rollback');
    return found ? { ...found } : null;
  }

  async listSnapshots(stream) {
    return this._snap.snapshots.filter((s) => s.stream === stream).map((s) => ({ ...s }));
  }

  async allSnapshots() {
    return this._snap.snapshots.map((s) => ({ ...s }));
  }

  async getSnapshot(snapshotId) {
    const s = this._snap.snapshots.find((x) => x.snapshotId === snapshotId);
    return s ? { ...s, state: s.state } : null;
  }

  async deleteSnapshot(snapshotId) {
    this._assertWritable();
    this._snap.snapshots = this._snap.snapshots.filter((s) => s.snapshotId !== snapshotId);
  }
}

function makeCursor(rows) {
  let i = 0;
  return {
    next: async () => {
      if (i >= rows.length) return null;
      return rows[i++];
    },
    [Symbol.asyncIterator]() {
      return {
        next: async () => {
          if (i >= rows.length) return { done: true, value: null };
          return { done: false, value: rows[i++] };
        }
      };
    },
    async all() {
      return rows;
    }
  };
}
