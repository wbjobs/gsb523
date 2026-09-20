import { EventStoreClient } from './lib/event-store-client.js';

const store = new EventStoreClient();
const streamId = 'demo';
const stateOutput = document.querySelector('#stateOutput');
const eventList = document.querySelector('#eventList');
const snapshotList = document.querySelector('#snapshotList');
const resultOutput = document.querySelector('#resultOutput');
const storageInfo = document.querySelector('#storageInfo');

const elements = {
  append: document.querySelector('#appendButton'),
  rollback: document.querySelector('#rollbackButton'),
  stateAt: document.querySelector('#stateAtButton'),
  snapshot: document.querySelector('#snapshotButton'),
  compact: document.querySelector('#compactButton'),
  benchmark: document.querySelector('#benchmarkButton'),
};

init();

elements.append.addEventListener('click', appendEvent);
elements.rollback.addEventListener('click', rollbackState);
elements.stateAt.addEventListener('click', queryStateAt);
elements.snapshot.addEventListener('click', createSnapshot);
elements.compact.addEventListener('click', compactSnapshots);
elements.benchmark.addEventListener('click', runBenchmark);

async function init() {
  await store.init({ snapshotEvery: 25, keepSnapshots: 5 });
  await refresh();
}

async function appendEvent() {
  const entityId = value('#entityId');
  const name = value('#entityName');
  const expectedVersion = Number(value('#expectedVersion'));
  const event = await store.append(streamId, 'entity.updated', { name }, {
    entityId,
    expectedVersion: expectedVersion === -1 ? null : expectedVersion,
    metadata: { source: 'demo-ui' },
  });

  document.querySelector('#expectedVersion').value = event.version;
  await showResult('事件已追加', event);
  await refresh();
}

async function rollbackState() {
  const targetVersion = Number(value('#rollbackVersion'));
  const result = await store.rollback(streamId, targetVersion, {
    reason: 'manual-demo-rollback',
  });
  await showResult('已创建回滚事件，当前祖先已改变', result);
  await refresh();
}

async function queryStateAt() {
  const atVersion = Number(value('#queryVersion'));
  const result = await store.stateAt(streamId, { atVersion });
  stateOutput.textContent = JSON.stringify(result, null, 2);
  await showResult(`已查询版本 ${atVersion}`, result);
}

async function createSnapshot() {
  const result = await store.createSnapshot(streamId);
  await showResult('快照已创建', summarizeSnapshot(result));
  await refresh();
}

async function compactSnapshots() {
  const result = await store.compactSnapshots(streamId, { keep: 3 });
  await showResult('旧快照已压缩删除', result);
  await refresh();
}

async function runBenchmark() {
  setBusy(true);
  const benchmarkStream = `benchmark-${Date.now()}`;
  const total = 100_000;
  const batchSize = 2_500;
  const measurements = {};

  try {
    const writeStart = performance.now();
    for (let offset = 0; offset < total; offset += batchSize) {
      const count = Math.min(batchSize, total - offset);
      await store.appendMany(
        benchmarkStream,
        Array.from({ length: count }, (_, index) => ({
          type: 'counter.incremented',
          entityId: 'counter',
          payload: { delta: 1, batch: offset + index },
        })),
        { snapshot: false, metadata: { benchmark: true } }
      );
    }
    measurements.writeMs = performance.now() - writeStart;

    const queryStart = performance.now();
    const queried = await store.query({
      kind: 'events',
      streamId: benchmarkStream,
      limit: total,
    });
    measurements.eventRangeQueryMs = performance.now() - queryStart;
    measurements.eventCount = queried.items.length;

    const snapshotStart = performance.now();
    const snapshot = await store.createSnapshot(benchmarkStream);
    measurements.snapshotMs = performance.now() - snapshotStart;

    const restoreStart = performance.now();
    const restored = await store.materialize(benchmarkStream);
    measurements.snapshotRestoreMs = performance.now() - restoreStart;
    measurements.finalCount = restored.state.entities?.counter?.count;

    const travelStart = performance.now();
    const traveled = await store.materialize(benchmarkStream, {
      atVersion: 50_000,
    });
    measurements.timeTravelMs = performance.now() - travelStart;
    measurements.travelCount = traveled.state.entities?.counter?.count;

    await showResult('基准完成', measurements);
  } finally {
    setBusy(false);
    await refreshStorageInfo();
  }
}

async function refresh() {
  const [state, events, snapshots] = await Promise.all([
    store.materialize(streamId),
    store.inspectEvents({ streamId, limit: 30 }),
    store.listSnapshots(streamId),
  ]);

  stateOutput.textContent = JSON.stringify(state, null, 2);
  eventList.innerHTML = events.items.map(renderEvent).join('');
  snapshotList.innerHTML = snapshots.map(renderSnapshot).join('') ||
    '<p class="hint">暂无快照</p>';

  document.querySelector('#rollbackVersion').max = Math.max(1, state.version - 1);
  document.querySelector('#queryVersion').max = state.version;
  await refreshStorageInfo();
}

async function refreshStorageInfo() {
  const info = await store.storageInfo();
  storageInfo.textContent = JSON.stringify(info, null, 2);
}

function renderEvent(event) {
  return `<div class="item">
    <strong>v${event.version} · ${event.type}</strong><br />
    ${event.entityId ? `entity: ${escapeHtml(event.entityId)}<br />` : ''}
    parent: ${event.parentId ? short(event.parentId) : '∅'}<br />
    <small>${new Date(event.timestamp).toLocaleString()}</small>
  </div>`;
}

function renderSnapshot(snapshot) {
  return `<div class="item">
    <strong>${short(snapshot.id)}</strong><br />
    event v${snapshot.eventVersion}<br />
    codec: ${snapshot.codec}<br />
    ${snapshot.originalBytes} → ${snapshot.stateBytes} bytes
  </div>`;
}

async function showResult(label, detail) {
  resultOutput.textContent = `${label}\n\n${JSON.stringify(detail, null, 2)}`;
}

function summarizeSnapshot(snapshot) {
  return {
    id: snapshot.id,
    streamId: snapshot.streamId,
    eventVersion: snapshot.eventVersion,
    codec: snapshot.codec,
    originalBytes: snapshot.originalBytes,
    compressedBytes: snapshot.compressedBytes,
    commit: snapshot.commit,
  };
}

function setBusy(isBusy) {
  for (const button of Object.values(elements)) button.disabled = isBusy;
}

function value(selector) {
  return document.querySelector(selector).value;
}

function short(id) {
  return id.length > 16 ? `${id.slice(0, 12)}…${id.slice(-4)}` : id;
}

function escapeHtml(input) {
  return String(input).replace(/[&<>"']/g, (char) => ({
    '&': '&amp;',
    '<': '&lt;',
    '>': '&gt;',
    '"': '&quot;',
    "'": '&#39;',
  }[char]));
}
