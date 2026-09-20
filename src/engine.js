import { fnv1aHex, byteLengthOf, isRollbackMarker, compileFunctionSource } from './util.js';
import { VersionConflictError, IntegrityError } from './errors.js';
import { encodeSnapshot, decodeSnapshot } from './snapshot.js';
import { QuotaManager } from './quota.js';

const META_SYSTEM = '__system__';
const META_HEAD_PREFIX = 'head:';
const META_STREAMS = '__streams__';
const META_QUOTA = '__quota__';
const META_SEQ = '__seq__';
const ROLLBACK_TYPE = '$rollback';
const ENVELOPE_FIELDS = ['id', 'stream', 'epoch', 'seq', 'globalSeq', 'type', 'timestamp', 'prevHash', 'meta'];

function defaultReducer(state, event) {
  if (state == null) return { eventsApplied: [] };
  state.eventsApplied.push(event.type);
  return state;
}

export class EventStore {
  constructor(adapter, options = {}) {
    this.adapter = adapter;
    this.reducers = new Map();
    this._mutex = Promise.resolve();
    this._heads = new Map();
    this._globalSeq = 0;
    this._streams = new Set();
    this.clock = options.clock || (() => Date.now());
    this.quota = new QuotaManager(adapter, options.quota || {});
    this.snapshotEvery = options.snapshotEvery ?? 1000;
    this.compressSnapshots = options.compressSnapshots !== false;
    this._initialized = false;
  }

  registerReducer(stream, reducer) {
    this.reducers.set(stream, reducer);
  }

  registerReducers(map) {
    for (const [stream, reducer] of Object.entries(map)) {
      this.registerReducer(stream, reducer);
    }
  }

  registerReducerSource(stream, source) {
    this.registerReducer(stream, compileFunctionSource(source, ['state', 'event']));
  }

  _reducerFor(stream) {
    return this.reducers.get(stream) || defaultReducer;
  }

  async open() {
    if (this._initialized) return this;
    await this.adapter.open();
    await this.adapter.runTransaction(['meta'], 'readonly', async (tx) => {
      const meta = tx.store('meta');
      const streamsRec = await meta.get(META_STREAMS);
      if (streamsRec && Array.isArray(streamsRec.value)) {
        this._streams = new Set(streamsRec.value);
      }
      for (const stream of this._streams) {
        const rec = await meta.get(META_HEAD_PREFIX + stream);
        if (rec) this._heads.set(stream, rec.value);
      }
      const seqRec = await meta.get(META_SEQ);
      this._globalSeq = seqRec ? seqRec.value : 0;
      const quotaRec = await meta.get(META_QUOTA);
      if (quotaRec) this.quota.loadMeta(quotaRec.value);
    });
    this._initialized = true;
    return this;
  }

  _withLock(fn) {
    const run = this._mutex.then(fn, fn);
    this._mutex = run.then(
      () => undefined,
      () => undefined
    );
    return run;
  }

  _head(stream) {
    const head = this._heads.get(stream);
    if (head) return head;
    const fresh = { stream, epoch: 0, seq: 0, globalSeq: 0, prevHash: null };
    this._heads.set(stream, fresh);
    return fresh;
  }

  async _saveMeta(tx, changedStreams = []) {
    const meta = tx.store('meta');
    await meta.put({ key: META_SEQ, value: this._globalSeq });
    await meta.put({ key: META_STREAMS, value: [...this._streams] });
    await meta.put({ key: META_QUOTA, value: this.quota.toMeta() });
    for (const stream of changedStreams) {
      await meta.put({ key: META_HEAD_PREFIX + stream, value: this._heads.get(stream) });
    }
  }

  /**
   * Append one event.
   *
   * expectedVersion semantics (per stream epoch seq):
   *   undefined  no check
   *   -1         stream must not exist in the current epoch
   *   n          current seq must equal n
   *
   * Returns the stored envelope.
   */
  append(stream, type, payload, expectedVersion) {
    return this._withLock(() => this._appendInternal(stream, type, payload, expectedVersion));
  }

  async _appendInternal(stream, type, payload, expectedVersion) {
    if (typeof stream !== 'string' || !stream) throw new TypeError('stream must be a non-empty string');
    if (typeof type !== 'string' || !type) throw new TypeError('type must be a non-empty string');
    if (isRollbackMarker(type)) {
      throw new TypeError(`event type "${type}" is reserved (system markers start with $)`);
    }
    const head = this._head(stream);
    if (expectedVersion === -1 && head.seq !== 0) {
      throw new VersionConflictError(stream, -1, head.seq);
    }
    if (typeof expectedVersion === 'number' && expectedVersion >= 0 && head.seq !== expectedVersion) {
      throw new VersionConflictError(stream, expectedVersion, head.seq);
    }

    const record = this._buildRecord(stream, head.epoch, head.seq + 1, type, payload, head.prevHash);
    const estimated = byteLengthOf(record);
    this.quota.assertWritable(estimated);

    try {
      await this.adapter.runTransaction(['events', 'meta'], 'readwrite', async (tx) => {
        await tx.store('events').put(record);
        this._advanceHead(head, record);
        this._streams.add(stream);
        await this._saveMeta(tx, [stream]);
      });
    } catch (error) {
      throw this.quota.handleStorageError(error, estimated);
    }
    this.quota.account(estimated);

    if (this.snapshotEvery > 0 && head.seq % this.snapshotEvery === 0) {
      await this._materialize(stream).catch(() => {});
      await this.quota.compactSnapshots(stream).catch(() => {});
    } else if (this.quota.needsCompaction && this.quota.mode === 'normal') {
      await this.quota.compactSnapshots(stream).catch(() => {});
    }
    return this._envelope(record);
  }

  /**
   * High-throughput bulk append for one stream. Every batch is one atomic
   * transaction; a version conflict aborts the whole call with nothing
   * committed.
   */
  async appendStream(stream, events, { expectedVersion, batchSize = 2000 } = {}) {
    return this._withLock(() => this._bulkInternal(stream, events, expectedVersion, batchSize));
  }

  async _bulkInternal(stream, events, expectedVersion, batchSize) {
    if (!Array.isArray(events) || events.length === 0) return [];
    const head = this._head(stream);
    if (typeof expectedVersion === 'number' && expectedVersion >= 0 && head.seq !== expectedVersion) {
      throw new VersionConflictError(stream, expectedVersion, head.seq);
    }
    if (expectedVersion === -1 && head.seq !== 0) {
      throw new VersionConflictError(stream, -1, head.seq);
    }
    const written = [];
    let prevHash = head.prevHash;
    let seq = head.seq;
    for (let i = 0; i < events.length; i += batchSize) {
      const batch = events.slice(i, i + batchSize);
      const records = batch.map((event) => {
        seq += 1;
        const rec = this._buildRecord(stream, head.epoch, seq, event.type, event.payload, prevHash);
        prevHash = rec.prevHash;
        written.push(rec);
        return rec;
      });
      const estimated = records.reduce((sum, rec) => sum + byteLengthOf(rec), 0);
      this.quota.assertWritable(estimated);
      try {
        await this.adapter.runTransaction(['events', 'meta'], 'readwrite', async (tx) => {
          await tx.store('events').putMany(records);
          this._advanceHead(head, records[records.length - 1]);
          this._streams.add(stream);
          await this._saveMeta(tx, [stream]);
        });
      } catch (error) {
        throw this.quota.handleStorageError(error, estimated);
      }
      this.quota.account(estimated);
    }
    // One snapshot for the whole bulk load; per-batch materialization would
    // replay an ever-growing stream and degrade to O(n^2).
    if (this.snapshotEvery > 0) {
      await this._materialize(stream).catch(() => {});
    }
    await this.quota.compactSnapshots(stream).catch(() => {});
    return written.map((rec) => this._envelope(rec));
  }

  _buildRecord(stream, epoch, seq, type, payload, prevHash) {
    this._globalSeq += 1;
    const timestamp = this.clock();
    const payloadHash = fnv1aHex(JSON.stringify(payload ?? null));
    const header = `${epoch}|${seq}|${type}|${timestamp}|${payloadHash}`;
    const hashInput = prevHash == null ? header : `${prevHash}.${header}`;
    const record = {
      id: `${stream}:${epoch}:${seq}`,
      stream,
      epoch,
      seq,
      globalSeq: this._globalSeq,
      type,
      timestamp,
      payload: payload ?? null,
      prevHash: fnv1aHex(hashInput),
      payloadHash,
      rollbackTo: null
    };
    return record;
  }

  _advanceHead(head, record) {
    head.epoch = record.epoch;
    head.seq = record.seq;
    head.globalSeq = record.globalSeq;
    head.prevHash = record.prevHash;
  }

  _envelope(record) {
    const out = {};
    for (const field of ENVELOPE_FIELDS) out[field] = record[field];
    out.payload = record.payload;
    out.rollbackTo = record.rollbackTo;
    return out;
  }

  /**
   * Roll a stream back to seq targetSeq. The log is append-only: instead of
   * deleting, a $rollback marker is appended and the stream forks into a new
   * epoch whose seq numbering restarts at 1. Every previous point stays
   * reachable via time travel.
   *
   * Options:
   *   force:true skips the optimistic version check (use when replaying
   *   already-persisted work).
   */
  rollback(stream, targetSeq, expectedVersion) {
    return this._withLock(() => this._rollbackInternal(stream, targetSeq, expectedVersion));
  }

  async _rollbackInternal(stream, targetSeq, expectedVersion) {
    if (!Number.isInteger(targetSeq) || targetSeq < 0) {
      throw new TypeError('targetSeq must be a non-negative integer');
    }
    const head = this._head(stream);
    if (!this._streams.has(stream) || head.seq === 0) {
      throw new IntegrityError(`cannot roll back unknown/empty stream "${stream}"`);
    }
    if (typeof expectedVersion === 'number' && expectedVersion >= 0 && head.seq !== expectedVersion) {
      throw new VersionConflictError(stream, expectedVersion, head.seq);
    }
    if (targetSeq >= head.seq) {
      throw new IntegrityError(`rollback target ${targetSeq} is not before head ${head.seq}`);
    }

    const marker = this._buildRecord(
      stream,
      head.epoch,
      head.seq + 1,
      ROLLBACK_TYPE,
      { newEpoch: head.epoch + 1, targetSeq },
      head.prevHash
    );
    marker.rollbackTo = targetSeq;
    marker.id = `${stream}:${head.epoch}:rollback`;

    const estimated = byteLengthOf(marker);
    this.quota.assertWritable(estimated);

    const oldEpoch = head.epoch;
    const targetGlobalSeq = await this._globalSeqOf(stream, oldEpoch, targetSeq);

    await this.adapter.runTransaction(['events', 'meta'], 'readwrite', async (tx) => {
      await tx.store('events').put(marker);
      head.epoch = oldEpoch + 1;
      head.seq = 0;
      head.globalSeq = marker.globalSeq;
      head.prevHash = null;
      this._streams.add(stream);
      await this._saveMeta(tx, [stream]);
    });
    this.quota.account(estimated);

    await this._materialize(stream, {
      baselineFrom: { stream, epoch: oldEpoch, seq: targetSeq, globalSeq: targetGlobalSeq }
    }).catch(() => {});

    return { stream, newEpoch: oldEpoch + 1, rolledFromEpoch: oldEpoch, targetSeq, marker: this._envelope(marker) };
  }

  async _globalSeqOf(stream, epoch, seq) {
    if (seq === 0) return null;
    const rec = await this.adapter.runTransaction(['events'], 'readonly', async (tx) =>
      tx.store('events').findStreamRecord(stream, epoch, seq)
    );
    if (!rec) throw new IntegrityError(`rollback anchor ${stream}@${epoch}#${seq} not found`);
    return rec.globalSeq;
  }

  /**
   * Build (or rebuild) the materialized view of the current head and persist
   * a snapshot. Reads happen in one transaction, gzip encoding after commit,
   * then the snapshot writes in its own transaction.
   */
  async _materialize(stream, options = {}) {
    const head = this._head(stream);
    const target = options.baselineFrom
      ? { epoch: head.epoch, seq: 0 }
      : { epoch: head.epoch, seq: head.seq };

    const { state, lastIncluded, base } = await this.adapter.runTransaction(
      ['events', 'snapshots'],
      'readonly',
      (tx) => this._readState(tx, stream, target.epoch, target.seq, options.baselineFrom)
    );

    const record = await encodeSnapshot({
      stream,
      epoch: target.epoch,
      baseSeq: target.seq,
      state,
      eventHash: lastIncluded ? lastIncluded.prevHash : (base ? base.eventHash : null),
      compress: this.compressSnapshots
    });
    await this.adapter.runTransaction(['snapshots'], 'readwrite', async (tx) => {
      await tx.store('snapshots').put(record);
    });
    return record;
  }

  async snapshot(stream) {
    return this._withLock(async () => {
      if (!this._streams.has(stream)) throw new IntegrityError(`unknown stream "${stream}"`);
      return this._materialize(stream);
    });
  }

  /**
   * Read materialized state at (epoch, seq), replaying from the best
   * snapshot. baselineFrom forces a replay ending at a specific physical
   * point (used to seed epoch N+1 from the rollback target of epoch N).
   */
  async _readState(tx, stream, epoch, seq, baselineFrom) {
    const ranges = await this._viewRanges(tx, stream);
    const snapshots = await tx.store('snapshots').listSnapshots(stream);

    if (baselineFrom) {
      const anchor = baselineFrom;
      const anchorSnap = snapshots
        .filter((s) => s.epoch === anchor.epoch && s.baseSeq <= anchor.seq)
        .sort((a, b) => b.baseSeq - a.baseSeq)[0];
      const playlist = [];
      let base = null;
      let initialState;
      if (anchorSnap) {
        base = anchorSnap;
        initialState = await decodeSnapshot(anchorSnap);
        playlist.push({
          epoch: anchor.epoch,
          fromSeq: anchorSnap.baseSeq + 1,
          toSeq: anchor.seq,
          untilGlobalSeq: anchor.globalSeq
        });
      } else {
        initialState = null;
        playlist.push({ epoch: anchor.epoch, fromSeq: 1, toSeq: anchor.seq, untilGlobalSeq: null });
      }
      const state = await this._replay(tx, stream, playlist, initialState);
      let anchorHash = null;
      if (anchor.seq > 0) {
        const anchorEvent = await tx.store('events').findStreamRecord(
          stream,
          anchor.epoch,
          anchor.seq
        );
        anchorHash = anchorEvent ? anchorEvent.prevHash : null;
      }
      return {
        state,
        lastIncluded: anchor.seq > 0 ? { prevHash: anchorHash } : null,
        base
      };
    }

    const eligible = snapshots
      .filter((s) => this._snapshotReaches(ranges, s, epoch, seq))
      .map((s) => ({ snap: s, cost: this._replayCost(ranges, s, epoch, seq) }))
      .sort((a, b) => a.cost - b.cost)[0];

    const playlist = [];
    let initialState = null;
    let base = null;
    if (eligible) {
      base = eligible.snap;
      initialState = await decodeSnapshot(eligible.snap);
      const startEpoch = eligible.snap.epoch;
      for (const range of ranges) {
        if (range.epoch < startEpoch) continue;
        const from = range.epoch === startEpoch ? eligible.snap.baseSeq + 1 : range.from;
        const to = range.epoch === epoch ? Math.min(range.to, seq) : range.to;
        if (from <= to) playlist.push({ epoch: range.epoch, fromSeq: from, toSeq: to });
        if (range.epoch === epoch) break;
      }
    } else {
      for (const range of ranges) {
        if (range.epoch > epoch) break;
        playlist.push({
          epoch: range.epoch,
          fromSeq: range.from,
          toSeq: range.epoch === epoch ? Math.min(range.to, seq) : range.to
        });
        if (range.epoch === epoch) break;
      }
    }

    const state = await this._replay(tx, stream, playlist, initialState);
    return { state, lastIncluded: null, base };
  }

  _snapshotReaches(ranges, snap, epoch, seq) {
    if (snap.epoch > epoch) return false;
    const rangeAt = ranges.find((r) => r.epoch === snap.epoch);
    if (!rangeAt) return false;
    if (snap.baseSeq > rangeAt.to) return false;
    if (snap.epoch === epoch) return snap.baseSeq <= seq;
    return true;
  }

  _replayCost(ranges, snap, epoch, seq) {
    let cost = 0;
    for (const range of ranges) {
      if (range.epoch < snap.epoch || range.epoch > epoch) continue;
      const from = range.epoch === snap.epoch ? snap.baseSeq + 1 : range.from;
      const to = range.epoch === epoch ? Math.min(range.to, seq) : range.to;
      if (from <= to) cost += to - from + 1;
    }
    return cost;
  }

  async _replay(tx, stream, playlist, initialState) {
    const reducer = this._reducerFor(stream);
    let state = initialState === null ? reducer(null, { type: '__init__' }) : initialState;
    const eventsStore = tx.store('events');
    for (const part of playlist) {
      const cursor = eventsStore.scanStreamRaw(stream, part.fromSeq, part.toSeq, [part.epoch]);
      for (;;) {
        const event = await cursor.next();
        if (event == null) break;
        state = reducer(state, this._envelope(event));
      }
    }
    return state;
  }

  /**
   * Visible ranges of one stream at the current head. Each entry is
   * { epoch, from, to } where `from` is the rollback target + 1 for
   * epochs forked from a rollback, else 1.
   */
  /**
   * Visible playlist of one stream at the current head: an ordered list of
   * physical windows. A rollback truncates the parent window to its target
   * and opens a fresh epoch window (seq restarts at 1).
   */
  async _viewRanges(tx, stream) {
    const playlist = [];
    const cursor = tx.store('events').scanStream(stream);
    for (;;) {
      const event = await cursor.next();
      if (event == null) break;
      if (event.type === ROLLBACK_TYPE) {
        const markerEpoch = event.epoch;
        const target = event.rollbackTo;
        while (playlist.length && playlist[playlist.length - 1].epoch > markerEpoch) {
          playlist.pop();
        }
        const parent = playlist.find((r) => r.epoch === markerEpoch);
        if (parent) parent.to = Math.min(parent.to, target);
        playlist.push({ epoch: markerEpoch + 1, from: 1, to: 0 });
      } else {
        let range = playlist.find((r) => r.epoch === event.epoch);
        if (!range) {
          range = { epoch: event.epoch, from: 1, to: 0 };
          playlist.push(range);
        }
        range.to = Math.max(range.to, event.seq);
      }
    }
    return playlist;
  }

  /**
   * Compute per-stream visible ranges at an arbitrary physical point
   * (time travel). Returns Map<stream, Array<{epoch, from, to}>>.
   */
  async _viewRangesAtGlobal(tx, globalPoint) {
    const map = new Map();
    const cursor = tx.store('events').scanEvents({ upper: globalPoint });
    for (;;) {
      const event = await cursor.next();
      if (event == null) break;
      let playlist = map.get(event.stream);
      if (!playlist) {
        playlist = [];
        map.set(event.stream, playlist);
      }
      if (event.type === ROLLBACK_TYPE) {
        const markerEpoch = event.epoch;
        while (playlist.length && playlist[playlist.length - 1].epoch > markerEpoch) {
          playlist.pop();
        }
        const parent = playlist.find((r) => r.epoch === markerEpoch);
        if (parent) parent.to = Math.min(parent.to, event.rollbackTo);
        playlist.push({ epoch: markerEpoch + 1, from: 1, to: 0 });
      } else {
        let range = playlist.find((r) => r.epoch === event.epoch);
        if (!range) {
          range = { epoch: event.epoch, from: 1, to: 0 };
          playlist.push(range);
        }
        range.to = Math.max(range.to, event.seq);
      }
    }
    for (const playlist of map.values()) playlist.sort((a, b) => a.epoch - b.epoch);
    return map;
  }

  /**
   * Query events.
   *
   * @param {object} opts
   *   stream        restrict to one stream
   *   type          filter by event type (string or array)
   *   filter        (event) => boolean
   *   filterSource  function source string (for Web Worker transport)
   *   atGlobalSeq   time travel: read as of this physical sequence number
   *   limit         maximum events (default 1000)
   *   includeSystem include $ markers (default false)
   */
  async query(opts = {}) {
    const options = { limit: 1000, includeSystem: false, ...opts };
    const filterFn = options.filter
      ? options.filter
      : options.filterSource
        ? compileFunctionSource(options.filterSource, ['event'])
        : null;
    const typeSet = options.type
      ? new Set(Array.isArray(options.type) ? options.type : [options.type])
      : null;

    return this.adapter.runTransaction(['events'], 'readonly', async (tx) => {
      const eventsStore = tx.store('events');
      const point = options.atGlobalSeq != null ? options.atGlobalSeq : this._globalSeq;
      const allRanges = await this._viewRangesAtGlobal(tx, point);
      const ranges = options.stream
        ? (allRanges.has(options.stream) ? allRanges.get(options.stream) : [])
        : allRanges;

      const out = [];
      const accept = (event) => {
        if (!options.includeSystem && isRollbackMarker(event.type)) return false;
        if (typeSet && !typeSet.has(event.type)) return false;
        if (options.stream && event.stream !== options.stream) return false;
        if (ranges && !(options.includeSystem && isRollbackMarker(event.type))) {
          const list = ranges instanceof Map ? ranges.get(event.stream) : ranges;
          if (!list) return false;
          const range = list.find((r) => r.epoch === event.epoch);
          if (!range || event.seq < range.from || event.seq > range.to) return false;
        }
        if (filterFn && !filterFn(this._envelope(event))) return false;
        return true;
      };

      const upper = options.atGlobalSeq ?? null;
      const cursor = options.stream
        ? eventsStore.scanStream(options.stream, upper)
        : eventsStore.scanEvents({ upper });

      for (;;) {
        const event = await cursor.next();
        if (event == null) break;
        if (accept(event)) {
          out.push(this._envelope(event));
          if (out.length >= options.limit) break;
        }
      }
      return out;
    });
  }

  /** Current logical head (does not include rollback markers). */
  getHead(stream) {
    const head = this._heads.get(stream);
    return head ? { ...head } : null;
  }

  listStreams() {
    return [...this._streams];
  }

  async getState(stream) {
    const head = this._head(stream);
    return this.adapter.runTransaction(['events', 'snapshots'], 'readonly', async (tx) => {
      const ranges = await this._viewRanges(tx, stream);
      const { state } = await this._readStateWithRanges(
        tx,
        stream,
        ranges,
        head.epoch,
        head.seq
      );
      return state;
    });
  }

  /** State of a stream as of an arbitrary physical point. */
  async getStateAt(stream, globalPoint) {
    return this.adapter.runTransaction(['events', 'snapshots'], 'readonly', async (tx) => {
      const rangesMap = await this._viewRangesAtGlobal(tx, globalPoint);
      const list = rangesMap.get(stream);
      if (!list || list.length === 0) return this._reducerFor(stream)(null, { type: '__init__' });
      const live = list[list.length - 1];
      const { state } = await this._readStateWithRanges(tx, stream, list, live.epoch, live.to);
      return state;
    });
  }

  async _readStateWithRanges(tx, stream, ranges, epoch, seq) {
    const snapshots = await tx.store('snapshots').listSnapshots(stream);
    const eligible = snapshots
      .filter((s) => this._snapshotReaches(ranges, s, epoch, seq))
      .map((s) => ({ snap: s, cost: this._replayCost(ranges, s, epoch, seq) }))
      .sort((a, b) => a.cost - b.cost)[0];

    const playlist = [];
    let initialState = null;
    if (eligible) {
      initialState = await decodeSnapshot(eligible.snap);
      for (const range of ranges) {
        if (range.epoch < eligible.snap.epoch) continue;
        const from = range.epoch === eligible.snap.epoch ? eligible.snap.baseSeq + 1 : range.from;
        const to = range.epoch === epoch ? Math.min(range.to, seq) : range.to;
        if (from <= to) playlist.push({ epoch: range.epoch, fromSeq: from, toSeq: to });
        if (range.epoch === epoch) break;
      }
    } else {
      for (const range of ranges) {
        if (range.epoch > epoch) break;
        playlist.push({
          epoch: range.epoch,
          fromSeq: range.from,
          toSeq: range.epoch === epoch ? Math.min(range.to, seq) : range.to
        });
      }
    }
    return { state: await this._replay(tx, stream, playlist, initialState) };
  }

  /** Physical high-water mark. */
  get physicalSeq() {
    return this._globalSeq;
  }

  /**
   * Verify one stream: physical hash chain integrity, rollback anchor
   * existence and snapshot checksums. Throws IntegrityError on first failure.
   */
  async verifyStream(stream) {
    return this.adapter.runTransaction(['events', 'snapshots'], 'readonly', async (tx) => {
      const eventsStore = tx.store('events');
      let prevHash = null;
      let count = 0;
      const cursor = eventsStore.scanStream(stream);
      for (;;) {
        const event = await cursor.next();
        if (event == null) break;
        count += 1;
        const payloadHash = fnv1aHex(JSON.stringify(event.payload ?? null));
        const header = `${event.epoch}|${event.seq}|${event.type}|${event.timestamp}|${payloadHash}`;
        const expected = fnv1aHex(prevHash == null ? header : `${prevHash}.${header}`);
        if (expected !== event.prevHash) {
          throw new IntegrityError(
            `hash chain broken at ${stream} gSeq=${event.globalSeq} (${event.epoch}#${event.seq})`
          );
        }
        if (payloadHash !== event.payloadHash) {
          throw new IntegrityError(`payload tampered at ${stream} gSeq=${event.globalSeq}`);
        }
        if (event.type === ROLLBACK_TYPE) {
          const target = event.rollbackTo;
          if (target == null) {
            throw new IntegrityError(`rollback marker missing anchor at gSeq=${event.globalSeq}`);
          }
          const anchor = await eventsStore.findStreamRecord(stream, event.epoch, target);
          if (!anchor) {
            throw new IntegrityError(
              `rollback anchor ${stream}@${event.epoch}#${target} does not exist`
            );
          }
          prevHash = null;
        } else {
          prevHash = event.prevHash;
        }
      }
      const snaps = await tx.store('snapshots').listSnapshots(stream);
      for (const snap of snaps) {
        await decodeSnapshot(snap);
      }
      return { stream, physicalEvents: count, snapshotsVerified: snaps.length, ok: true };
    });
  }
};

