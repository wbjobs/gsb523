# Event Store — IndexedDB + Web Worker 事件溯源存储

Append-only 事件日志，支持快照压缩、回滚（epoch 分叉）、时间旅行查询、乐观并发控制与存储配额降级。核心逻辑运行在 Web Worker 中，持久化使用 IndexedDB。

## 架构

```
页面 (public/demo.js)
   │  postMessage RPC（结构化克隆；reducer/filter 以函数字符串传输后在 Worker 内编译）
   ▼
EventStoreClient (src/client.js)
   ▼
Worker: EventStoreServer (src/worker-server.js, src/worker.js)
   ▼
EventStore (src/engine.js)          并发互斥锁 / 乐观版本 / 回滚分叉 / 时间旅行 / 查询引擎
   ├── Snapshot   (src/snapshot.js) gzip + checksum
   ├── Quota      (src/quota.js)    软阈值 GC / 硬配额只读降级
   └── Storage    (src/adapters/)   IndexedDbAdapter（生产）/ MemoryAdapter（测试）
```

## 数据模型

事件（append-only，永不删除）：

```
{ id, stream, epoch, seq, globalSeq, type, timestamp,
  payload, payloadHash, prevHash, rollbackTo }
```

- `globalSeq`：全局物理位点，单调递增，时间旅行用它定位。
- `(stream, epoch, seq)`：流内逻辑位点；`seq` 在每个 epoch 内从 1 开始。
- `prevHash`：`FNV-1a(prevHash . epoch|seq|type|timestamp|payloadHash)`，跨回滚边界重置；`verifyStream` 校验整条物理哈希链、回滚锚点与快照 checksum。

快照：

```
{ snapshotId, stream, epoch, baseSeq, state(gzip+json Uint8Array),
  stateChecksum, rawBytes, compressedBytes, eventHash }
```

## 回滚语义（不删日志）

回滚到 `seq=k` 时追加一条 `$rollback` 标记，并把流分叉到新 epoch：

- 新 epoch 以父 epoch 的 `#1..#k` 状态为基线，`seq` 从 1 重新计数；
- 当前视图是一个“播放列表”：继承窗口（父 epoch 截断到 k）+ 新 epoch 窗口；
- 分叉点之前的一切仍可通过 `getStateAt / query({ atGlobalSeq })` 访问；
- 回滚后自动写一份 epoch 基线快照，恢复路径 = 选最优快照 + 仅回放窗口增量。

## 并发与一致性

- Worker 内单 EventStore，所有写操作过一把 Promise 互斥锁串行化；
- 乐观版本：`append(stream, type, payload, expectedVersion)`，`-1` 表示仅新建，冲突抛 `VERSION_CONFLICT`（带 expected/actual），调用方可重试；
- 读操作使用单个 IDB 事务的一致性快照，**不会读到未提交的批次中间态**（只可能看到批前或批后）；
- 批量写 `appendStream` 每批一个原子事务（默认 2000/批）。

## 配额与降级（事件永不删除）

- `QuotaManager` 按写入字节计数，软阈值（默认 80%）自动压缩快照：保留每 epoch 基线 + 每流最新 N 份，其余删除；
- 达到硬配额：写抛 `QuotaExceededError`，随后进入只读模式（`ReadOnlyError`），读继续可用；
- 底层 `QuotaExceededError` 也会被映射到同一降级策略；释放空间后 `resume()` 恢复。

## 查询引擎

`store.query({ stream?, type?, filter|filterSource?, atGlobalSeq?, limit=1000, includeSystem? })`

- 沿 `[stream,epoch,seq]` 复合索引 / stream 索引游标扫描，按可见播放列表过滤；
- 支持类型过滤与谓词过滤；`$` 开头的系统标记默认隐藏；
- 时间旅行：传入任意历史 `globalSeq`，按该物理点重放当时的可见性。

## 运行

```bash
npm test          # Node 内存适配器 + fake-IDB 驱动真实 IndexedDB 适配器
npm run bench     # 10 万事件写入/查询/时间旅行基准
npm run serve     # http://localhost:8123/public/ 浏览器 Demo（真实 IDB + Worker）
```

## 验收对照

| 标准 | 实现与测试 |
| --- | --- |
| 可回滚到任意事件点 | `rollback(stream, seq)` + `tests/rollback.test.js` |
| 快照后恢复正确 | gzip/checksum 快照、回滚基线、重开恢复 `tests/snapshot.test.js`、`tests/idb-adapter.test.js` |
| 10 万事件查询 < 1s | `tests/bench.test.js`（内存适配器全量/过滤/谓词/时间旅行均 < 250ms） |
| 并发写不脏读 | 写锁串行化 + 事务快照读 `tests/concurrency.test.js` |
| 配额不足有降级 | 软阈值自动 GC、硬配额只读、读不中断 `tests/quota.test.js` |

## 文件

- `src/engine.js` — 事件追加、版本控制、回滚分叉、快照物化、时间旅行、查询、完整性校验
- `src/snapshot.js` / `src/quota.js` / `src/util.js` / `src/errors.js`
- `src/adapters/idb.js` — IndexedDB 适配器（复合索引、游标、批量 putMany）
- `src/adapters/memory.js` — 同契约内存适配器
- `src/worker.js` / `src/worker-server.js` / `src/client.js` / `src/protocol.js`
- `tests/` — 回滚、快照、并发、配额、RPC、IDB 适配器（fake-idb）、10 万事件基准
- `public/` — 浏览器演示页面
