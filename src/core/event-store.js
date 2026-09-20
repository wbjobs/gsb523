import {
  DEFAULT_STREAM,
  ROLLBACK_TYPE,
  SYSTEM_EVENT_PREFIX,
} from './protocol.js';
import { ConcurrencyConflictError } from './errors.js';
import { hashEvent, hashValue } from './canonical.js';
import { compressJson, decompressJson } from './compression.js';
import { resolveReducer } from './reducers.js';
import { KeyedMutex } from './locks.js';
import { activeSegments, replayAt } from './ancestry.js';

export class EventStore {
  constructor(storage, options = {}) {
    this.storage = storage;
    this.locks = new KeyedMutex();
    this.reducers = options.reducers || new Map();
    this.snapshotEvery = options.snapshotEvery ?? 1000;
    this.keepSnapshots = options.keepSnapshots ?? 3;
    this.reducerVersions = new Map(Object.entries(options.reducerVersions || {}));
  }

  configureReducer(streamId, reducer) {
    this.reducers.set(streamId || '*', reducer);
  }

  async append(streamId = DEFAULT_STREAM, type, payload = {}, options = {}) {
    const results = await this.appendMany(streamId, [
      { type, payload, ...options },
    ], options);
    return results[0];
  }

  async appendMany(streamId = DEFAULT_STREAM, entries = [], options = {}) {
    if (!entries.length) return [];
    if (entries.some((entry) => !entry.type || typeof entry.type !== 'string')) {
      throw new Error('Every event requires a string type');
    }
    if (
      entries.some((entry) => entry.type.startsWith(SYSTEM_EVENT_PREFIX))
    ) {
      throw new Error('System event types are reserved');
    }

    return this.locks.run(`stream:${streamId}`, async () => {
      const stream = await this.storage.getStream(streamId);
      const expectedVersion = options.expectedVersion ?? null;

      if (
        expectedVersion !== null &&
        expectedVersion !== undefined &&
        expectedVersion !== -1 &&
        stream.version !== expectedVersion
      ) {
        throw new ConcurrencyConflictError(
          `Expected stream ${streamId} at ${expectedVersion}, found ${stream.version}`,
          {
            streamId,
            expectedVersion,
            currentVersion: stream.version,
          }
        );
      }

      const now = options.timestamp ?? Date.now();
      const events = [];
      let version = stream.version;
      let parentId = stream.headId;
      let parentHash = stream.headHash || null;
      const rewritesAt = stream.rewrites.map(cloneRewrite);

      for (let index = 0; index < entries.length; index += 1) {
        const entry = entries[index];
        version += 1;
        const event = {
          id: entry.id || createId('evt'),
          streamId,
          version,
          type: entry.type,
          payload: entry.payload || {},
          entityId: entry.entityId ?? null,
          metadata: entry.metadata || {},
          timestamp: entry.timestamp ?? now + index,
          expectedVersion: version - 1,
          parentId,
          parentHash,
          rewritesAt: cloneRewrites(rewritesAt),
        };
        event.hash = hashEvent(event);
        events.push(event);
        parentId = event.id;
        parentHash = event.hash;
      }

      const updatedAt = events[events.length - 1].timestamp;
      const nextStream = {
        ...stream,
        id: streamId,
        version,
        headId: parentId,
        headHash: parentHash,
        updatedAt,
      };

      try {
        await this.storage.commitEvents(events, [nextStream]);
      } catch (error) {
        if (error.name === 'ConcurrencyConflictError') throw error;
        throw new ConcurrencyConflictError(
          `Concurrent write rejected for ${streamId}`,
          {
            streamId,
            currentVersion: stream.version,
            cause: error.message,
          }
        );
      }

      const last = events[events.length - 1];
      const distance = version - (stream.stateHeadEventVersion ?? 0);
      if (
        options.snapshot !== false &&
        (options.forceSnapshot ||
          (this.snapshotEvery > 0 && distance >= this.snapshotEvery))
      ) {
        try {
          await this.createSnapshotInternal(streamId, { atEventId: last.id });
        } catch {
          // Events remain durable; a later explicit snapshot or compaction can retry.
        }
      }

      return events;
    });
  }

  async rollback(streamId = DEFAULT_STREAM, targetVersion, options = {}) {
    return this.locks.run(`stream:${streamId}`, async () => {
      const stream = await this.storage.getStream(streamId);
      if (!Number.isInteger(targetVersion) || targetVersion < 1) {
        throw new Error('Rollback targetVersion must be a positive event version');
      }
      if (targetVersion >= stream.version) {
        throw new Error('Rollback target must be before the current version');
      }

      const targetEvents = await this.storage.getEventsRange(
        streamId,
        targetVersion,
        targetVersion
      );
      const target = targetEvents[0];
      if (!target) throw new Error(`Event version ${targetVersion} not found`);
      if (target.type.startsWith(SYSTEM_EVENT_PREFIX)) {
        throw new Error('Cannot roll back to a system event');
      }

      const version = stream.version + 1;
      const marker = {
        version,
        targetId: target.id,
        targetVersion: target.version,
        targetTimestamp: target.timestamp,
      };
      const rewritesAt = [...(target.rewritesAt || []), marker];
      const rollbackEvent = {
        id: options.id || createId('evt'),
        streamId,
        version,
        type: ROLLBACK_TYPE,
        payload: {
          targetId: target.id,
          targetVersion: target.version,
          targetTimestamp: target.timestamp,
        },
        entityId: null,
        metadata: options.metadata || { reason: options.reason || 'rollback' },
        timestamp: options.timestamp ?? Date.now(),
        expectedVersion: stream.version,
        parentId: stream.headId,
        parentHash: stream.headHash || null,
        rewritesAt,
      };
      rollbackEvent.hash = hashEvent(rollbackEvent);

      const nextStream = {
        ...stream,
        version,
        headId: rollbackEvent.id,
        headHash: rollbackEvent.hash,
        rewrites: rewritesAt,
        stateHeadId: null,
        stateHeadEventVersion: null,
        updatedAt: rollbackEvent.timestamp,
      };

      try {
        await this.storage.commitEvents([rollbackEvent], [nextStream]);
      } catch (error) {
        throw new ConcurrencyConflictError(
          `Rollback conflict for ${streamId}`,
          { cause: error.message }
        );
      }

      let snapshot = null;
      try {
        snapshot = await this.createSnapshotInternal(streamId, {
          atEventId: rollbackEvent.id,
        });
      } catch (error) {
        if (error.name === 'QuotaExhaustedError') {
          try {
            await this.compactSnapshots(streamId);
          } catch {
            // Retry still returns the original quota failure if nothing can be reclaimed.
          }
          snapshot = await this.createSnapshotInternal(streamId, {
            atEventId: rollbackEvent.id,
          });
        } else {
          throw error;
        }
      }

      return { event: rollbackEvent, target, snapshot };
    });
  }

  async materialize(streamId = DEFAULT_STREAM, options = {}) {
    return this.locks.run(`read:${streamId}`, async () => {
      const stream = await this.storage.getStream(streamId);
      const target = await this.resolveTarget(stream, options);

      if (target.version === 0) {
        return {
          state: {},
          version: 0,
          eventId: null,
          timestamp: stream.createdAt || 0,
          snapshotUsed: null,
          applied: 0,
          segments: [],
        };
      }

      verifyEvent(target);
      const snapshots = await this.storage.getSnapshotCandidates(streamId);
      for (const snapshot of snapshots) verifySnapshotIntegrity(snapshot);

      const result = await replayAt({
        storage: this.storage,
        streamId,
        target,
        targetRewrites: target.rewritesAt ?? [],
        reducer: resolveReducer(this.reducers, streamId),
        snapshots,
        verifyEvent,
        restoreSnapshot: async (snapshot) => {
          const state = await decompressJson(snapshot);
          if (hashValue(state) !== snapshot.stateHash) {
            throw new Error(`Snapshot state mismatch: ${snapshot.id}`);
          }
          return state;
        },
      });

      return {
        ...result,
        state: result.state,
        version: target.version,
        eventId: target.id,
        timestamp: target.timestamp,
      };
    });
  }

  stateAt(streamId, options) {
    return this.materialize(streamId, options);
  }

  async createSnapshot(streamId = DEFAULT_STREAM, options = {}) {
    return this.locks.run(`stream:${streamId}`, async () => {
      return this.createSnapshotInternal(streamId, options);
    });
  }

  async createSnapshotInternal(streamId = DEFAULT_STREAM, options = {}) {
    const materialized = await this.materialize(streamId, options);
    const stream = await this.storage.getStream(streamId);
    const target = await this.resolveTarget(stream, options);
    const compressed = await compressJson(materialized.state);
    const id = options.id || createId('snap');
    const snapshot = {
      id,
      streamId,
      eventId: target.id,
      eventVersion: target.version,
      stateHash: hashValue(materialized.state),
      codec: compressed.codec,
      stateBytes: compressed.bytes,
      originalBytes: compressed.originalBytes,
      compressedBytes: compressed.bytes.byteLength,
      parentStateHeadId: stream.stateHeadId || null,
      reducerVersion:
        this.reducers.get(streamId)?.version ||
        this.reducerVersions.get(streamId) ||
        1,
      createdAt: Date.now(),
    };
    snapshot.hash = hashSnapshot(snapshot);

    const nextStream = {
      ...stream,
      stateHeadId: id,
      stateHeadEventVersion: target.version,
      updatedAt: target.timestamp,
    };
    const commit = await this.storage.commitSnapshot(snapshot, nextStream);

    return {
      ...snapshot,
      stateBytes: snapshot.stateBytes,
      commit,
    };
  }

  async listSnapshots(streamId = null) {
    return this.storage.listSnapshots(streamId);
  }

  async compactSnapshots(streamId = null, options = {}) {
    const keep = options.keep ?? this.keepSnapshots;
    const snapshots = await this.storage.listSnapshots(streamId);
    const streamIds = new Set(
      snapshots.map((snapshot) => snapshot.streamId)
    );
    const deleted = [];

    for (const currentStreamId of streamIds) {
      if (streamId && currentStreamId !== streamId) continue;

      await this.locks.run(`read:${currentStreamId}`, async () => {
        const stream = await this.storage.getStream(currentStreamId);
        const group = snapshots
          .filter((snapshot) => snapshot.streamId === currentStreamId)
          .sort((left, right) => right.eventVersion - left.eventVersion);

        for (const [index, snapshot] of group.entries()) {
          const protectedSnapshot =
            index < keep || snapshot.id === stream.stateHeadId;
          if (!protectedSnapshot) {
            await this.storage.deleteSnapshot(snapshot.id);
            deleted.push(snapshot.id);
          }
        }
      });
    }

    return { deleted, kept: snapshots.length - deleted.length };
  }

  queryEvents(filters = {}) {
    return this.storage.queryEvents({
      includeSystem: false,
      ...filters,
    });
  }

  inspectEvents(filters = {}) {
    return this.storage.queryEvents({
      includeSystem: true,
      ...filters,
    });
  }

  storageInfo() {
    return this.storage.storageInfo();
  }

  async resolveTarget(stream, options) {
    if (options.atEventId) {
      const events = await this.storage.getEventsByIds([options.atEventId]);
      const event = events[0];
      if (!event || event.streamId !== stream.id) {
        throw new Error(`Event ${options.atEventId} not found`);
      }
      return event;
    }

    if (Number.isInteger(options.atVersion)) {
      if (options.atVersion === 0) {
        return { id: null, version: 0, rewritesAt: [], timestamp: 0 };
      }
      const events = await this.storage.getEventsRange(
        stream.id,
        options.atVersion,
        options.atVersion
      );
      const event = events[0];
      if (!event) throw new Error(`Version ${options.atVersion} not found`);
      return event;
    }

    if (!stream.headId) {
      return { id: null, version: 0, rewritesAt: stream.rewrites, timestamp: 0 };
    }
    const events = await this.storage.getEventsByIds([stream.headId]);
    return events[0];
  }
}

function verifyEvent(event) {
  if (!event.hash || hashEvent(event) !== event.hash) {
    throw new Error(`Event integrity check failed: ${event.id}`);
  }
}

function verifySnapshotIntegrity(snapshot) {
  if (!snapshot.hash || hashSnapshot(snapshot) !== snapshot.hash) {
    throw new Error(`Snapshot integrity check failed: ${snapshot.id}`);
  }
}

function hashSnapshot(snapshot) {
  const { hash, ...integrityInput } = snapshot;
  return hashValue(integrityInput);
}

function cloneRewrite(rewrite) {
  return { ...rewrite };
}

function cloneRewrites(rewrites) {
  return rewrites.map(cloneRewrite);
}

function createId(prefix) {
  if (globalThis.crypto?.randomUUID) {
    return `${prefix}_${globalThis.crypto.randomUUID()}`;
  }
  return `${prefix}_${Date.now().toString(36)}_${Math.random()
    .toString(36)
    .slice(2, 12)}`;
}
