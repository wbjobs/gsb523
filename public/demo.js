import { EventStoreClient } from '../src/client.js';

const counterSource = `(state, event) => {
  if (state == null) state = { count: 0 };
  if (event.type === 'inc') state.count += (event.payload && event.payload.n) || 1;
  if (event.type === 'dec') state.count -= (event.payload && event.payload.n) || 1;
  return state;
}`;

const benchSource = `(state, event) => {
  if (state == null) state = { sum: 0, n: 0 };
  state.sum += event.payload.value;
  state.n += 1;
  return state;
}`;

const $ = (id) => document.getElementById(id);
const out = (id, text, cls) => {
  const node = $(id);
  node.textContent = text;
  node.className = cls || '';
};
const timed = async (fn) => {
  const start = performance.now();
  const result = await fn();
  return { result, ms: Math.round(performance.now() - start) };
};

let client;

async function boot() {
  client = await EventStoreClient.create({
    dbName: 'event-store-demo',
    snapshotEvery: 10000,
    compressSnapshots: true,
    quota: { maxBytes: 400 * 1024 * 1024, snapshotsPerStream: 3 },
    reducers: { counter: counterSource, bench: benchSource }
  });
  const quota = await client.estimateQuota();
  const streams = await client.listStreams();
  out('benchOut', `已打开 IndexedDB。已有 stream：${streams.join(', ') || '(无)'}\n配额模式：${quota.mode}`);
}

$('bench').onclick = async () => {
  $('bench').disabled = true;
  try {
    const TOTAL = 100000;
    const events = Array.from({ length: TOTAL }, (_, i) => ({
      type: 'tick',
      payload: { value: i % 7, idx: i }
    }));
    const w = await timed(() =>
      client.appendStream('bench', events, { expectedVersion: -1, batchSize: 5000 })
    );
    const q = await timed(() => client.query({ stream: 'bench', limit: TOTAL }));
    const ttPoint = Math.floor((await client.physicalSeq) / 2);
    const tt = await timed(() => client.query({ stream: 'bench', atGlobalSeq: ttPoint, limit: TOTAL }));
    const quota = await client.estimateQuota();
    out(
      'benchOut',
      `写入 ${TOTAL} 条：${w.ms} ms（批量 5000/事务）\n` +
        `全量查询 ${q.result.length} 条：${q.ms} ms ${q.ms < 1000 ? '✅ <1s' : '❌'}\n` +
        `时间旅行 @${ttPoint}：${tt.result.length} 条 / ${tt.ms} ms ${tt.ms < 1000 ? '✅' : '❌'}\n` +
        `追踪占用：${(quota.tracked / 1024 / 1024).toFixed(2)} MB，模式 ${quota.mode}`,
      q.ms < 1000 ? 'ok' : 'bad'
    );
  } catch (error) {
    out('benchOut', String(error.stack || error), 'bad');
  } finally {
    $('bench').disabled = false;
  }
};

$('clear').onclick = async () => {
  indexedDB.deleteDatabase('event-store-demo');
  location.reload();
};

$('append10').onclick = async () => {
  for (let i = 0; i < 10; i++) await client.append('counter', 'inc');
  const head = await client.getHead('counter');
  out('rbOut', `已追加。head = epoch ${head.epoch} seq ${head.seq}`);
};

$('snap').onclick = async () => {
  const snap = await client.snapshot('counter');
  out(
    'rbOut',
    `快照已保存：epoch ${snap.epoch} baseSeq ${snap.baseSeq}\n` +
      `原始 ${snap.rawBytes}B → gzip ${snap.compressedBytes}B（checksum ${snap.stateChecksum}）`
  );
};

$('rollback').onclick = async () => {
  const target = Number($('rbSeq').value);
  try {
    const result = await client.rollback('counter', target);
    const state = await client.getState('counter');
    const verify = await client.verifyStream('counter');
    out(
      'rbOut',
      `回滚到 seq ${target} → 新 epoch ${result.newEpoch}\n` +
        `恢复状态 count=${state.count}（基线快照 + 回放）\n` +
        `完整性校验：${verify.ok ? '✅ hash 链与快照 checksum 均通过' : '❌'}`
    );
  } catch (error) {
    out('rbOut', String(error.stack || error), 'bad');
  }
};

$('state').onclick = async () => {
  const state = await client.getState('counter');
  const head = await client.getHead('counter');
  out('rbOut', `head = epoch ${head.epoch} seq ${head.seq}\nstate = ${JSON.stringify(state)}`);
};

$('tt').onclick = async () => {
  const point = Number($('ttPoint').value);
  const [state, rows] = await Promise.all([
    client.getStateAt('counter', point),
    client.query({ stream: 'counter', atGlobalSeq: point, limit: 100000 })
  ]);
  $('ttHint').textContent = `当前物理高水位 ${await client.physicalSeq}`;
  out('ttOut', `@${point}：可见事件 ${rows.length} 条，count=${state.count}`);
};

$('concurrent').onclick = async () => {
  $('concurrent').disabled = true;
  try {
    const before = await client.getState('counter');
    const startCount = before ? before.count : 0;
    const results = await Promise.allSettled(
      Array.from({ length: 50 }, () => client.append('counter', 'inc'))
    );
    const okCount = results.filter((r) => r.status === 'fulfilled').length;
    const after = await client.getState('counter');
    out(
      'concOut',
      `50 个并发 append：成功 ${okCount}，串行化后无丢失\n` +
        `count ${startCount} → ${after.count}（差 ${after.count - startCount}）${after.count - startCount === okCount ? ' ✅' : ' ❌'}`,
      'ok'
    );
  } finally {
    $('concurrent').disabled = false;
  }
};

$('quota').onclick = async () => {
  await client.setQuotaMode('readonly');
  try {
    await client.append('counter', 'inc');
    out('concOut', '只读后写入竟然成功 ❌', 'bad');
  } catch (error) {
    out(
      'concOut',
      `已降级为只读：写入被拒（${error.name}: ${error.message}）\n查询仍可用：`,
      'ok'
    );
    const rows = await client.query({ stream: 'counter', limit: 5 });
    $('concOut').textContent += `\n最近可见事件 ${rows.length} 条 ✅`;
  }
};

$('resume').onclick = async () => {
  await client.setQuotaMode('normal');
  out('concOut', '已恢复读写模式。');
};

boot().catch((error) => out('benchOut', String(error.stack || error), 'bad'));
