# 事件溯源存储引擎

零依赖静态 Web 应用。所有写操作都是不可变事件，默认运行在 Web Worker 中，使用 IndexedDB 原子事务持久化；浏览器配额不足时自动把受影响流降级到内存溢写层并继续读写。

## 能力

- **追加事件**：事件包含 `version`、`parentId`、`parentHash`、`rewritesAt`，按流做乐观并发控制。
- **回滚**：创建 `__rollback__` 事件，不删除旧事件；可回到当前祖先，也可恢复已经被前一次回滚取代的历史事件点。
- **时间旅行**：按 `atVersion` 或 `atEventId` 计算目标事件当时的祖先段并重放。
- **快照一致性**：快照记录事件 ID/版本、状态哈希、父快照、codec、压缩前后大小；恢复前校验事件链和快照状态哈希。
- **压缩**：浏览器支持时使用 `CompressionStream('gzip')`，否则降级为 canonical JSON。
- **并发写**：Worker 内按流串行化，IndexedDB 事务再次校验 `expectedVersion`；冲突返回 `ConcurrencyConflictError`。
- **查询引擎**：提供事件索引查询、当前状态和任意版本状态查询；IndexedDB 使用流/版本/类型/实体复合索引。
- **配额降级**：主存储抛出 `QuotaExceededError` 后，该流切换到内存溢写后端；读取合并主存储和溢写层，存储状态标记为 `degraded`。

## 快速开始

```bash
# 任选一个静态服务器
python3 -m http.server 8080
# 打开 http://localhost:8080
```

运行自动化验证：

```bash
npm test
```

测试使用内存适配器执行同样的核心逻辑，覆盖版本、回滚、快照、冲突、批处理原子性、配额降级和 10 万事件性能。

## 核心文件

- `src/core/event-store.js`：事件追加、回滚、快照、时间旅行入口。
- `src/core/ancestry.js`：根据事件记录的 `rewritesAt` 计算祖先段。
- `src/core/indexeddb-storage.js`：IndexedDB schema、索引和原子事务。
- `src/core/memory-storage.js`：内存适配器、配额模拟和主/溢写混合存储。
- `src/core/compression.js`：gzip/JSON 快照压缩。
- `src/core/query-engine.js`：事件与状态查询封装。
- `src/workers/event-store.worker.js`：Worker RPC 和存储实例宿主。
- `src/lib/event-store-client.js`：主线程 Promise RPC 客户端。
- `index.html` 与 `src/app.js`：验收演示和 10 万事件基准。

## 事件与快照模型

事件示例：

```json
{
  "id": "evt_...",
  "streamId": "orders",
  "version": 3,
  "type": "order.paid",
  "payload": {},
  "entityId": "order-1",
  "expectedVersion": 2,
  "parentId": "evt_previous",
  "parentHash": "8f1a...",
  "rewritesAt": [],
  "hash": "c91b..."
}
```

回滚事件的 `payload.targetId` 指向目标事件；该目标事件自己的 `rewritesAt` 加上本次回滚标记，成为回滚后新事件点的祖先链。这样历史版本仍可查询，新分支也不会污染旧状态。

快照示例：

```json
{
  "id": "snap_...",
  "streamId": "orders",
  "eventId": "evt_...",
  "eventVersion": 300,
  "stateHash": "f2...",
  "codec": "gzip+json",
  "stateBytes": "Uint8Array",
  "parentStateHeadId": "snap_..."
}
```

## API

```js
const client = new EventStoreClient();
await client.init({ snapshotEvery: 1000, keepSnapshots: 5 });

const event = await client.append(
  'orders',
  'order.opened',
  { total: 100 },
  { entityId: 'order-1', expectedVersion: -1 }
);

await client.appendMany('orders', events, { expectedVersion: 10 });
await client.rollback('orders', 42);

const current = await client.materialize('orders');
const historical = await client.stateAt('orders', { atVersion: 42 });
const events = await client.queryEvents({
  streamId: 'orders',
  types: ['order.paid'],
  entityId: 'order-1',
  fromVersion: 1,
  limit: 50,
});

const snapshot = await client.createSnapshot('orders');
await client.compactSnapshots('orders', { keep: 5 });
const info = await client.storageInfo();
```

`expectedVersion: -1` 或 `null` 表示不做乐观版本检查；指定版本时，版本不一致会失败且不会产生脏写。

## 10 万事件验收

页面中的“运行 10 万事件基准”使用独立 `benchmark-*` 流：

1. 分批追加 100,000 个事件。
2. 按 `streamVersion` 复合索引做一次全量范围查询。
3. 创建压缩快照。
4. 从快照恢复当前状态，确认应用事件数为 0。
5. 查询版本 50,000，确认计数为 50,000。

当前 Node 内存适配器参考结果（硬件相关）：

- 索引范围查询：约 `230ms`
- 快照恢复：约 `235ms`
- 快照后时间旅行到 50,000：约 `474ms`

## 配额与降级说明

浏览器对 IndexedDB 的配额限制和回收策略因浏览器、磁盘空间、站点持久化授权而异。本实现不在应用层“截断事件”，因为这会破坏任意历史点回滚能力；配额不足时：

1. 先尝试压缩/删除旧快照，保留当前快照和最近若干快照。
2. 写事件仍失败则把该流后续事件与快照写入内存溢写层。
3. 查询自动合并两层数据，不出现已提交事件丢失。
4. `storageInfo().mode` 返回 `degraded`，页面显示降级流。

内存溢写层在页面刷新后不保证保留；生产环境应在检测到降级时提示用户授权持久化存储、导出数据或释放空间，然后把溢写层转回 IndexedDB。

## IndexedDB Schema

- `events`
  - key: `id`
  - indexes: `streamVersion`、`streamTypeVersion`、`streamEntityVersion`、`streamTimestamp`
- `snapshots`
  - key: `id`
  - indexes: `streamEventVersion`、`streamIdOnly`
- `streams`
  - key: `id`
- `meta`
  - key: `key`
