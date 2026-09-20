import { test } from 'node:test';
import assert from 'node:assert/strict';
import { createClient } from './helpers.js';
import { VersionConflictError } from '../src/errors.js';

test('RPC client round-trips writes, queries, time travel and rollback', async () => {
  const { client } = await createClient({ snapshotEvery: 2 });
  const e1 = await client.append('counter', 'set', { value: 10 }, -1);
  assert.equal(e1.seq, 1);
  await client.append('counter', 'inc', { n: 5 });
  let state = await client.getState('counter');
  assert.equal(state.count, 15);

  const mid = await client.physicalSeq;
  await client.append('counter', 'inc', { n: 100 });
  assert.equal((await client.getState('counter')).count, 115);
  assert.equal((await client.getStateAt('counter', mid)).count, 15);

  await client.rollback('counter', 1);
  state = await client.getState('counter');
  assert.equal(state.count, 10);

  const rows = await client.query({ stream: 'counter' });
  assert.deepEqual(rows.map((r) => r.type), ['set']);

  const result = await client.verifyStream('counter');
  assert.equal(result.ok, true);
});

test('RPC propagates typed errors', async () => {
  const { client } = await createClient();
  await client.append('counter', 'inc');
  await assert.rejects(
    () => client.append('counter', 'inc', null, 99),
    (err) => err instanceof VersionConflictError && err.code === 'VERSION_CONFLICT'
  );
});

test('RPC supports bulk append and quota estimation', async () => {
  const { client } = await createClient({ snapshotEvery: 0, quota: { maxBytes: 10_000_000 } });
  const events = Array.from({ length: 5000 }, (_, i) => ({
    type: i % 3 === 0 ? 'inc' : 'dec'
  }));
  await client.appendStream('counter', events, { expectedVersion: -1, batchSize: 1000 });
  const head = await client.getHead('counter');
  assert.equal(head.seq, 5000);
  const quota = await client.estimateQuota();
  assert.ok(quota.tracked > 0);
  assert.equal(quota.mode, 'normal');
});
