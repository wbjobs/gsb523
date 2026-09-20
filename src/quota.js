import { byteLengthOf } from './util.js';
import { QuotaExceededError, ReadOnlyError } from './errors.js';

/**
 * Quota enforcement and degradation policy.
 *
 * Usage is tracked explicitly as records are written (cheaper than measuring
 * the whole database) and reconciled lazily. Three modes:
 *
 *  - normal       writes allowed; when usage crosses the soft threshold the
 *                 oldest non-baseline snapshots are compacted
 *  - compacting   one GC pass has run; writes still allowed while it helped
 *  - readonly     hard threshold reached (or the backing store reported
 *                 quota): reads allowed, writes rejected
 *
 * Events are never deleted: the log stays complete so every historical point
 * remains reachable and tamper-evident.
 */
export class QuotaManager {
  constructor(adapter, options = {}) {
    this.adapter = adapter;
    this.maxBytes = options.maxBytes ?? 256 * 1024 * 1024;
    this.softRatio = options.softRatio ?? 0.8;
    this.snapshotsPerStream = options.snapshotsPerStream ?? 3;
    this.mode = 'normal';
    this.usageBytes = 0;
  }

  get softLimit() {
    return Math.floor(this.maxBytes * this.softRatio);
  }

  /** Sync reservation check before a write transaction runs. */
  assertWritable(estimatedBytes) {
    if (this.mode === 'readonly') {
      throw new ReadOnlyError(
        `store degraded to read-only (usage ${this.usageBytes}/${this.maxBytes} bytes)`
      );
    }
    if (this.usageBytes + estimatedBytes > this.maxBytes) {
      throw new QuotaExceededError(
        `write of ${estimatedBytes} bytes would exceed quota ${this.maxBytes}`,
        { usage: this.usageBytes, quota: this.maxBytes, estimated: estimatedBytes }
      );
    }
  }

  account(bytes) {
    this.usageBytes += bytes;
  }

  release(bytes) {
    this.usageBytes = Math.max(0, this.usageBytes - bytes);
  }

  /** True when the GC compaction policy should run before another write. */
  get needsCompaction() {
    return this.usageBytes >= this.softLimit;
  }

  /**
   * Delete redundant snapshots, keeping:
   *  - the newest snapshot per epoch (needed for fast time travel)
   *  - the baseline (baseSeq 0) of every epoch
   *  - at most snapshotsPerStream snapshots overall
   * Candidates are removed oldest-first.
   */
  async compactSnapshots(stream = null) {
    let freed = 0;
    await this.adapter.runTransaction(['snapshots'], 'readwrite', async (tx) => {
      const snaps = tx.store('snapshots');
      const all = stream ? await snaps.listSnapshots(stream) : await snaps.allSnapshots();
      const byStream = new Map();
      for (const snap of all) {
        if (!byStream.has(snap.stream)) byStream.set(snap.stream, []);
        byStream.get(snap.stream).push(snap);
      }
      for (const group of byStream.values()) {
        group.sort((a, b) =>
          a.epoch !== b.epoch ? b.epoch - a.epoch : b.baseSeq - a.baseSeq
        );
        const keep = new Set();
        const newestPerEpoch = new Map();
        for (const snap of group) {
          if (snap.baseSeq === 0) keep.add(snap.snapshotId);
          if (!newestPerEpoch.has(snap.epoch)) newestPerEpoch.set(snap.epoch, snap);
        }
        for (const snap of newestPerEpoch.values()) keep.add(snap.snapshotId);
        let kept = 0;
        for (const snap of group) {
          if (kept >= this.snapshotsPerStream) break;
          keep.add(snap.snapshotId);
          kept += 1;
        }
        for (const snap of group) {
          if (!keep.has(snap.snapshotId)) {
            await snaps.deleteSnapshot(snap.snapshotId);
            freed += snap.bytes ?? byteLengthOf(snap);
          }
        }
      }
    });
    this.release(freed);
    if (this.usageBytes >= this.maxBytes) this.mode = 'readonly';
    return freed;
  }

  /** Map a low-level storage error onto the degradation policy. */
  handleStorageError(error, estimatedBytes) {
    const name = error && error.name;
    if (name === 'QuotaExceededError' || name === 'NS_ERROR_DOM_QUOTA_REACHED') {
      if (this.mode !== 'readonly') this.mode = 'compacting';
      throw new QuotaExceededError('backing storage reported quota exceeded', {
        usage: this.usageBytes,
        quota: this.maxBytes,
        estimated: estimatedBytes,
        cause: error
      });
    }
    throw error;
  }

  enterReadOnly() {
    this.mode = 'readonly';
  }

  resume() {
    this.mode = this.usageBytes >= this.maxBytes ? 'readonly' : 'normal';
  }

  async estimate() {
    if (typeof navigator !== 'undefined' && navigator.storage && navigator.storage.estimate) {
      try {
        const est = await navigator.storage.estimate();
        return {
          tracked: this.usageBytes,
          browserUsage: est.usage ?? null,
          browserQuota: est.quota ?? null,
          mode: this.mode
        };
      } catch {
        /* fall through to tracked values */
      }
    }
    return { tracked: this.usageBytes, browserUsage: null, browserQuota: null, mode: this.mode };
  }

  toMeta() {
    return { usageBytes: this.usageBytes, mode: this.mode };
  }

  loadMeta(meta) {
    if (meta && Number.isFinite(meta.usageBytes)) this.usageBytes = meta.usageBytes;
    if (meta && meta.mode) this.mode = meta.mode;
  }
}

