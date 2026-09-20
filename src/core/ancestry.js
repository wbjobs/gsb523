import { ROLLBACK_TYPE } from './protocol.js';

export function activeSegments(version, rewrites) {
  if (version <= 0) return [];

  const history = [...rewrites]
    .filter((rewrite) => rewrite.version <= version)
    .sort((left, right) => left.version - right.version);

  if (history.length === 0) {
    return [[1, version]];
  }

  const latest = history[history.length - 1];
  if (version === latest.version) {
    const beforeTarget = rewrites.filter(
      (rewrite) => rewrite.version < latest.targetVersion
    );
    return [
      ...activeSegments(latest.targetVersion, beforeTarget),
      [latest.version, latest.version],
    ];
  }

  return [
    ...activeSegments(latest.version, history),
    [latest.version + 1, version],
  ];
}

export function isActiveVersion(version, rewrites, headVersion) {
  return activeSegments(headVersion, rewrites).some(
    ([from, to]) => version >= from && version <= to
  );
}

export async function replayAt({
  storage,
  streamId,
  target,
  reducer,
  snapshots,
  restoreSnapshot,
  verifyEvent,
  targetRewrites,
}) {
  const rewrites = targetRewrites || [];
  const segments = activeSegments(target.version, rewrites);
  const ranges = segments.map(([from, to]) =>
    storage.getEventsRange(streamId, from, to)
  );
  const segmentEvents = (await Promise.all(ranges)).flat().sort(byVersion);
  const eventsByVersion = new Map(
    segmentEvents.map((event) => [event.version, event])
  );

  const usableSnapshots = snapshots
    .filter((snapshot) => snapshot.eventVersion <= target.version)
    .filter(
      (snapshot) =>
        snapshot.eventVersion === 0 ||
        eventsByVersion.get(snapshot.eventVersion)?.id === snapshot.eventId
    )
    .sort((left, right) => right.eventVersion - left.eventVersion);

  let state = {};
  let baseVersion = 0;
  let snapshotUsed = null;

  for (const snapshot of usableSnapshots) {
    const event = eventsByVersion.get(snapshot.eventVersion);
    const eventMatches =
      snapshot.eventVersion === 0 || event?.id === snapshot.eventId;
    if (!eventMatches) continue;

    state = await restoreSnapshot(snapshot);
    baseVersion = snapshot.eventVersion;
    snapshotUsed = {
      id: snapshot.id,
      eventVersion: snapshot.eventVersion,
      codec: snapshot.codec,
    };
    break;
  }

  let applied = 0;
  for (const event of segmentEvents) {
    if (event.version <= baseVersion) continue;
    verifyEvent?.(event);
    if (event.type === ROLLBACK_TYPE) continue;
    state = reducer(state, event) ?? state;
    applied += 1;
  }

  return {
    state,
    applied,
    snapshotUsed,
    segments,
  };
}

function byVersion(left, right) {
  return left.version - right.version;
}
