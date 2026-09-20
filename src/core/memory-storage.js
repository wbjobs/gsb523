import { QuotaExhaustedError } from './errors.js';
import { hashValue, stableStringify } from './canonical.js';
import { DEFAULT_STREAM } from './protocol.js';

const EXCLUDE_SYSTEM = (event) => !event.type.startsWith('__');

export class MemoryStorage {
  constructor(options = {}) {
    this.quotaBytes = options.quotaBytes ?? Infinity;
    this.name = options.name ?? 'memory';
    this.events = new Map();
    this.snapshots = new Map();
    this.streams = new Map();
    this.meta = new Map([
      ['version', 1],
      ['bytes', 0],
    ]);
    this.usedBytes = 0;
  }

  async getMeta(key, fallback = null) {
    return this.meta.has(key) ? this.meta.get(key) : fallback;
  }

  async setMeta(key, value) {
    this.meta.set(key, value);
  }

  getStreamSync(streamId) {
    return this.streams.get(streamId) || null;
  }

  getSnapshotSync(id) {
    return this.snapshots.get(id) || null;
  }

  getEventSync(id) {
    return this.events.get(id) || null;
  }

  async getStream(streamId) {
    return clone(this.getStreamSync(streamId) || defaultStream(streamId));
  }

  async getEventsByIds(ids) {
    return ids.map((id) => clone(this.getEventSync(id))).filter(Boolean);
  }

  async getEventsByStream(streamId, filters = {}) {
    const events = [...this.events.values()]
      .filter((event) => event.streamId === streamId)
      .filter(filters.includeSystem ? Boolean : EXCLUDE_SYSTEM)
      .sort(byVersion);

    return clone(
      applyEventWindow(events, filters).map((event) =>
        filters.keysOnly ? event.id : event
      )
    );
  }

  async getEventsRange(streamId, fromVersion, toVersion) {
    const events = [...this.events.values()]
      .filter(
        (event) =>
          event.streamId === streamId &&
          event.version >= fromVersion &&
          event.version <= toVersion
      )
      .sort(byVersion);
    return clone(events);
  }

  async queryEvents(filters = {}) {
    const includeSystem = Boolean(filters.includeSystem);
    let events = [...this.events.values()].sort(byVersion);

    if (filters.streamId) events = events.filter((event) => event.streamId === filters.streamId);
    if (!includeSystem) events = events.filter(EXCLUDE_SYSTEM);
    if (filters.types?.length) events = events.filter((event) => filters.types.includes(event.type));
    if (filters.entityId) events = events.filter((event) => event.entityId === filters.entityId);
    if (Number.isFinite(filters.fromVersion)) events = events.filter((event) => event.version >= filters.fromVersion);
    if (Number.isFinite(filters.toVersion)) events = events.filter((event) => event.version <= filters.toVersion);
    if (filters.from) events = events.filter((event) => event.timestamp >= filters.from);
    if (filters.to) events = events.filter((event) => event.timestamp <= filters.to);

    const count = events.length;
    events = applyEventWindow(events, filters);

    return {
      items: clone(events),
      count,
      plan: {
        index: filters.streamId ? 'streamVersion' : 'global',
        postFilter: true,
      },
    };
  }

  async commitEvents(events, nextStreams) {
    const addedBytes = events.reduce(
      (total, event) => total + approximateBytes(event),
      0
    );
    if (this.usedBytes + addedBytes > this.quotaBytes) {
      throw new QuotaExhaustedError('Storage quota exhausted', {
        neededBytes: addedBytes,
        availableBytes: Math.max(0, this.quotaBytes - this.usedBytes),
      });
    }

    const expectedVersions = new Map();
    for (const streamId of new Set(events.map((event) => event.streamId))) {
      expectedVersions.set(
        streamId,
        this.streams.get(streamId)?.version || 0
      );
    }

    for (const event of [...events].sort(byVersion)) {
      if (this.events.has(event.id)) {
        throw new Error(`Duplicate event id: ${event.id}`);
      }
      if (expectedVersions.get(event.streamId) !== event.expectedVersion) {
        throw new Error(`Stream ${event.streamId} changed before commit`);
      }
      expectedVersions.set(event.streamId, event.version);
    }

    for (const event of events) this.events.set(event.id, clone(event));
    for (const stream of nextStreams) this.streams.set(stream.id, clone(stream));
    this.usedBytes += addedBytes;
    await this.setMeta('bytes', this.usedBytes);
  }

  async commitSnapshot(snapshot, nextStream = null) {
    if (nextStream) {
      const current = this.streams.get(nextStream.id);
      const expectedVersion = nextStream.stateHeadEventVersion;
      if (current && current.version !== expectedVersion) {
        throw new Error(`Stream ${nextStream.id} changed before snapshot commit`);
      }
    }

    const addedBytes = approximateBytes(snapshot);
    const replacement = nextStream?.stateHeadId
      ? findSnapshotBytes(this.snapshots, snapshot.streamId, nextStream.stateHeadId, snapshot.id)
      : 0;

    if (this.usedBytes + addedBytes - replacement > this.quotaBytes) {
      throw new QuotaExhaustedError('Cannot store snapshot within quota', {
        neededBytes: addedBytes,
        availableBytes: Math.max(0, this.quotaBytes - this.usedBytes),
      });
    }

    this.snapshots.set(snapshot.id, clone(snapshot));
    if (nextStream) this.streams.set(nextStream.id, clone(nextStream));
    this.usedBytes += addedBytes - replacement;
    await this.setMeta('bytes', this.usedBytes);
  }

  async getSnapshotCandidates(streamId) {
    return clone(
      [...this.snapshots.values()]
        .filter((snapshot) => snapshot.streamId === streamId)
        .sort((left, right) => right.eventVersion - left.eventVersion)
    );
  }

  async deleteSnapshot(id) {
    const snapshot = this.snapshots.get(id);
    if (snapshot) {
      this.snapshots.delete(id);
      this.usedBytes -= approximateBytes(snapshot);
      await this.setMeta('bytes', this.usedBytes);
    }
  }

  async listSnapshots(streamId = null) {
    return clone(
      [...this.snapshots.values()]
        .filter((snapshot) => !streamId || snapshot.streamId === streamId)
        .sort((left, right) => right.createdAt - left.createdAt)
        .map(({ stateBytes, ...metadata }) => ({
          ...metadata,
          stateBytes: stateBytes.byteLength,
        }))
    );
  }

  async storageInfo() {
    return {
      backend: this.name,
      mode: 'normal',
      usedBytes: this.usedBytes,
      quotaBytes: this.quotaBytes,
      eventCount: this.events.size,
      streamCount: this.streams.size,
      snapshotCount: this.snapshots.size,
      degradedStreams: [],
    };
  }
}

export class OverflowStorage {
  constructor(primary, fallback) {
    this.primary = primary;
    this.fallback = fallback;
    this.degradedStreams = new Set();
  }

  async getStream(streamId) {
    const degraded = this.degradedStreams.has(streamId);
    const primaryStream = await this.primary.getStream(streamId);
    const fallbackStream = await this.fallback.getStream(streamId);

    if (degraded) {
      if (fallbackStream.version > 1 || fallbackStream.headId) {
        return fallbackStream;
      }
      return primaryStream;
    }

    if (fallbackStream.version > 1 || fallbackStream.headId) {
      return fallbackStream;
    }
    return primaryStream;
  }

  async getEventsByIds(ids) {
    const fallbackEvents = await this.fallback.getEventsByIds(ids);
    const fallbackIds = new Set(fallbackEvents.map((event) => event.id));
    const primaryEvents = await this.primary.getEventsByIds(
      ids.filter((id) => !fallbackIds.has(id))
    );
    return [...primaryEvents, ...fallbackEvents].sort(byVersion);
  }

  async getEventsByStream(streamId, filters = {}) {
    const [primaryEvents, fallbackEvents] = await Promise.all([
      this.primary.getEventsByStream(streamId, filters),
      this.fallback.getEventsByStream(streamId, filters),
    ]);
    return mergeEvents(primaryEvents, fallbackEvents);
  }

  async getEventsRange(streamId, fromVersion, toVersion) {
    const [primaryEvents, fallbackEvents] = await Promise.all([
      this.primary.getEventsRange(streamId, fromVersion, toVersion),
      this.fallback.getEventsRange(streamId, fromVersion, toVersion),
    ]);
    return mergeEvents(primaryEvents, fallbackEvents);
  }

  async queryEvents(filters = {}) {
    const [primaryResult, fallbackResult] = await Promise.all([
      this.primary.queryEvents(filters),
      this.fallback.queryEvents(filters),
    ]);

    const items = mergeEvents(primaryResult.items, fallbackResult.items);
    const count = uniqueEvents(primaryResult.items, fallbackResult.items).length;
    return {
      items: applyEventWindow(items, filters),
      count,
      plan: {
        index: 'multi-backend',
        postFilter: true,
      },
    };
  }

  async commitEvents(events, nextStreams) {
    const streamIds = new Set(events.map((event) => event.streamId));
    const alreadyDegraded = [...streamIds].some((streamId) =>
      this.degradedStreams.has(streamId)
    );

    if (!alreadyDegraded) {
      try {
        await this.primary.commitEvents(events, nextStreams);
        return { degraded: false, backend: 'primary' };
      } catch (error) {
        if (error.name !== 'QuotaExhaustedError') {
          throw new ConcurrencyConflictError(error.message, {
            cause: error.name,
          });
        }
      }
    }

    await Promise.all(
      [...nextStreams].map((stream) => this.seedStream(stream.id))
    );
    await this.fallback.commitEvents(events, nextStreams);
    for (const streamId of streamIds) this.degradedStreams.add(streamId);
    return { degraded: true, backend: 'fallback' };
  }

  async commitSnapshot(snapshot, nextStream = null) {
    const degraded = this.degradedStreams.has(snapshot.streamId);
    if (!degraded) {
      try {
        await this.primary.commitSnapshot(snapshot, nextStream);
        return { degraded: false, backend: 'primary' };
      } catch (error) {
        if (error.name !== 'QuotaExhaustedError') throw error;
      }
    }

    if (nextStream) await this.seedStream(nextStream.id);
    await this.fallback.commitSnapshot(snapshot, nextStream);
    this.degradedStreams.add(snapshot.streamId);
    return { degraded: true, backend: 'fallback' };
  }

  async getSnapshotCandidates(streamId) {
    const [primary, fallback] = await Promise.all([
      this.primary.getSnapshotCandidates(streamId),
      this.fallback.getSnapshotCandidates(streamId),
    ]);
    return mergeSnapshots(primary, fallback);
  }

  async deleteSnapshot(id) {
    const primary = this.primary.getSnapshotSync?.(id);
    const fallback = this.fallback.getSnapshotSync?.(id);
    if (primary) await this.primary.deleteSnapshot(id);
    if (fallback) await this.fallback.deleteSnapshot(id);
  }

  async listSnapshots(streamId = null) {
    const [primary, fallback] = await Promise.all([
      this.primary.listSnapshots(streamId),
      this.fallback.listSnapshots(streamId),
    ]);
    return [...primary, ...fallback].sort(
      (left, right) => right.createdAt - left.createdAt
    );
  }

  async storageInfo() {
    const [primary, fallback] = await Promise.all([
      this.primary.storageInfo(),
      this.fallback.storageInfo(),
    ]);
    return {
      backend: 'hybrid',
      mode: this.degradedStreams.size ? 'degraded' : 'normal',
      primary,
      fallback,
      usedBytes: primary.usedBytes + fallback.usedBytes,
      quotaBytes: primary.quotaBytes,
      eventCount: primary.eventCount + fallback.eventCount,
      streamCount: primary.streamCount + fallback.streamCount,
      snapshotCount: primary.snapshotCount + fallback.snapshotCount,
      degradedStreams: [...this.degradedStreams],
    };
  }

  async seedStream(streamId) {
    const [primaryStream, fallbackStream] = await Promise.all([
      this.primary.getStream(streamId),
      this.fallback.getStream(streamId),
    ]);

    if (!fallbackStream.headId && primaryStream.headId) {
      await this.fallback.commitEvents([], [primaryStream]);
    } else if (!fallbackStream.headId) {
      const now = Date.now();
      await this.fallback.commitEvents([], [{
        id: streamId,
        version: 0,
        headId: null,
        headHash: null,
        rewrites: [],
        stateHeadId: null,
        stateHeadEventVersion: null,
        createdAt: now,
        updatedAt: now,
      }]);
    }
  }
}

function defaultStream(streamId) {
  return {
    id: streamId || DEFAULT_STREAM,
    version: 0,
    headId: null,
    stateHeadId: null,
    rewrites: [],
    createdAt: 0,
    updatedAt: 0,
  };
}

function applyEventWindow(events, filters) {
  const offset = filters.offset ?? 0;
  if (filters.limit == null) return events.slice(offset);
  return events.slice(offset, offset + filters.limit);
}

function byVersion(left, right) {
  const streamOrder = left.streamId.localeCompare(right.streamId);
  return streamOrder || left.version - right.version;
}

function mergeEvents(primary, fallback) {
  return uniqueEvents(primary, fallback).sort(byVersion);
}

function uniqueEvents(primary, fallback) {
  const merged = new Map();
  for (const event of primary) merged.set(event.id, event);
  for (const event of fallback) merged.set(event.id, event);
  return [...merged.values()];
}

function mergeSnapshots(primary, fallback) {
  const merged = new Map();
  for (const snapshot of primary) merged.set(snapshot.id, snapshot);
  for (const snapshot of fallback) merged.set(snapshot.id, snapshot);
  return [...merged.values()].sort(
    (left, right) => right.snapshotVersion - left.snapshotVersion
  );
}

function findSnapshotBytes(snapshots, streamId, stateHeadId, newId) {
  for (const snapshot of snapshots.values()) {
    if (
      snapshot.streamId === streamId &&
      snapshot.id !== newId &&
      snapshot.eventId === stateHeadId
    ) {
      return approximateBytes(snapshot);
    }
  }
  return 0;
}

function approximateBytes(value) {
  const bytes = new TextEncoder().encode(stableStringify(value)).length;
  if (typeof value === 'object' && value !== null && value.hash == null) {
    return hashValue(value).length + bytes;
  }
  return bytes;
}

function clone(value) {
  if (value === null || value === undefined) return value;
  return globalThis.structuredClone
    ? globalThis.structuredClone(value)
    : JSON.parse(JSON.stringify(value));
}
