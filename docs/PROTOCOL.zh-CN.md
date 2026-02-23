# DedupFS Python ↔ Rust 数据库协议

[English](./PROTOCOL.md) | [简体中文](./PROTOCOL.zh-CN.md)

## 1. 范围

本文档定义 Python 控制平面与 Rust 数据平面共享的权威 DB-first 协议。

协议目标：
- 确定性行为
- 可审计状态迁移
- 迁移期兼容安全
- 策略（Python）与执行（Rust）严格分层

## 2. 枚举持久化规则（强制）

SQLite 中所有枚举类字段必须持久化为**小写枚举 value**（不是枚举 name）。

强制规则：
- Python ORM（`SAEnum`）必须写入枚举 `.value`。
- Rust SQL 读写必须使用相同的小写 value。
- 迁移逻辑在重建协议敏感约束前，必须把历史大写值标准化为小写。

## 3. 合法值集合

### 3.1 `jobs` 表协议值

| 字段 | 合法值 |
|---|---|
| `kind` | `scan`, `hash`, `delete`, `thumbnail` |
| `status` | `pending`, `running`, `completed`, `failed`, `cancelled`, `retryable` |

### 3.2 `scan_sessions` 与 `library_files`

| 表 | 字段 | 合法值 |
|---|---|---|
| `scan_sessions` | `status` | `running`, `succeeded`, `failed` |
| `library_files` | `hash_algorithm` | `blake3`, `sha256` |

### 3.3 `thumbnails` 与 `thumbnail_cleanup_jobs`

| 表 | 字段 | 合法值 |
|---|---|---|
| `thumbnails` | `status` | `pending`, `running`, `ready`, `failed` |
| `thumbnails` | `media_type` | `image`, `video` |
| `thumbnails` | `format` | `jpeg`, `webp` |
| `thumbnail_cleanup_jobs` | `status` | `pending`, `running`, `completed`, `failed` |

### 3.4 `wal_maintenance_jobs`

| 字段 | 合法值 |
|---|---|
| `requested_mode` | `passive`, `restart`, `truncate` |
| `status` | `pending`, `running`, `retryable`, `completed`, `failed` |

## 4. Lease / Heartbeat 语义

### 4.1 通用租约字段

以下字段属于租约契约字段：
- `worker_id`
- `worker_heartbeat_at`
- `lease_expires_at`

租约过期判定：
- 对应行应处于运行态时，若 `lease_expires_at IS NULL OR lease_expires_at <= now()`，即视为 stale。

### 4.2 `jobs`（scan/hash）租约语义

- claim：`pending -> running` 并绑定租约归属字段。
- heartbeat：租约所有者刷新 `worker_heartbeat_at` 并延长 `lease_expires_at`。
- finish：终态时清空 `lease_expires_at`。
- recover：Python 控制平面恢复和 Rust claim 路径恢复都可将 stale 的 `running` scan/hash 任务归类为 `retryable`，清空租约绑定字段，并写入确定性恢复错误元数据。

### 4.3 `thumbnails` 租约语义

- Python 的队列准入在容量策略下为原子化流程（单 DB 条件插入路径）。
- Rust 将一个 `pending` 缩略图行 claim 到 `running` 并持有租约。
- Rust 在缩略图生成期间持续刷新租约。
- Rust claim 路径必须对过期的 `running` 行进行回收（`running -> pending`，清空租约绑定字段）。
- 成功结束：`running -> ready` 并清空租约过期字段。
- 失败结束：`running -> failed`，落库 `error_code/error_message` 与 `retry_after`，并清空租约过期字段。
- 重试行为：仅当到达 `retry_after` 后，Python 才可把失败行重新入队为 `pending`。

### 4.4 `thumbnail_cleanup_jobs` 租约语义

- Python 以 `pending` + `execute_after` 调度清理任务。
- Rust claim 到期的清理行（`execute_after <= now`）进入 `running` 并持有租约，但仅在同一 `group_key` 下不存在 `pending/running` 缩略图行时才可 claim。
- Rust claim 路径必须对过期的 `running` 清理行进行回收（`running -> pending`，清空租约绑定字段）。
- Rust 完成时置为 `completed`，失败时置为 `failed` 并写入错误元数据。
- 清理执行遵循终态安全：仅删除 `ready/failed` 缩略图行。

### 4.5 `wal_maintenance_jobs` 租约语义

- Python 以 `pending` + `execute_after` 入队 WAL 维护任务。
- Rust 将到期的 `pending/retryable` 行 claim 为 `running` 并持有租约。
- Rust claim 路径必须回收 stale 的 `running` 行（`running -> retryable`），并写入确定性 `error_code/error_message`、递增 `retry_count`、设置 `retry_after`。
- Rust 执行 `PRAGMA wal_checkpoint(mode)` 并落库 checkpoint 统计字段：
  - `checkpoint_busy`
  - `checkpoint_log_frames`
  - `checkpointed_frames`
- 若 checkpoint busy（`checkpoint_busy > 0`），任务转 `retryable` 并带退避时间（`retry_after`），同时保留审计信息。
- 成功结束转 `completed`；执行错误转 `failed`。

## 5. scan/hash 单活跃互斥

数据库层不变量：
- 任意时刻最多只能有 1 条 scan/hash 任务处于 `pending` 或 `running`。

实现方式：
- 在 `jobs((1))` 上建立唯一部分索引 `ix_jobs_single_active_scan_hash`，谓词为：
  - `lower(status) IN ('pending', 'running') AND lower(kind) IN ('scan', 'hash')`

迁移期恢复策略：
- 重建该索引前，迁移必须：
  - 将枚举标准化为小写；
  - 识别重复 `running` scan/hash；
  - 保留确定性胜者；
  - 其余 `running` 任务转 `retryable` 并写恢复错误码；
  - 为满足单活跃约束，必要时将多余 `pending` scan/hash 也转 `retryable`；
  - 最后重建唯一索引。

## 6. 分页游标契约

### 6.1 jobs 列表分页

jobs 列表分页采用确定性 keyset 游标：
- 排序：`created_at DESC, id DESC`
- 游标锚点：上一页尾元素 `id` 对应的数据库行
- 下一页过滤条件：
  - `created_at < anchor.created_at`
  - 或 `created_at = anchor.created_at AND id < anchor.id`

稳定性保证：
- 跨页不重复
- 不因时间戳不唯一产生缺口
- 高并发插入下仍具确定性遍历

### 6.2 重复组分页

重复组列表接口（`GET /api/v1/duplicates/groups`）对聚合结果做 keyset 分页：
- 数据来源：`library_files` 中满足以下条件的行：
  - `is_missing = 0`
  - `needs_hash = 0`
  - `hash_algorithm IS NOT NULL`
  - `content_hash IS NOT NULL`
- 分组键：`(hash_algorithm, content_hash)`
- 仅返回 `COUNT(*) > 1` 的分组
- 排序：`file_count DESC, total_size_bytes DESC, hash_algorithm ASC, content_hash_hex ASC`
- 游标负载（base64url JSON）：
  - `file_count`
  - `total_size_bytes`
  - `hash_algorithm`
  - `content_hash_hex`

分组键格式：
- `group_key = <hash_algorithm>:<content_hash_hex>`
- `content_hash_hex` 必须匹配算法摘要长度：
  - `sha256`：64 个十六进制字符
  - `blake3`：64 个十六进制字符
- 例如：`sha256:4b227777d4dd1fc61c6f884f48641d02...`

稳定性保证：
- 在稳定快照下跨页不重复
- 通过 `(hash_algorithm, content_hash_hex)` 作为最终 tie-breaker 保持确定性顺序

### 6.3 重复组内文件分页

重复组文件接口（`GET /api/v1/duplicates/groups/{group_key}/files`）采用 keyset 分页：
- 过滤条件：
  - `is_missing = 0`
  - `needs_hash = 0`
  - `hash_algorithm = 解析出的 group_key.algorithm`
  - `content_hash = 解析出的 group_key.hash_hex`
- 排序：`id ASC`
- 游标：上一页尾元素 `id`（字符串整数）

迁移安全说明：
- migration `0011_duplicate_group_query_indexes` 在记录版本前必须保证 `library_files` 必要列齐全。
- 当历史 schema 缺失 `is_missing/needs_hash/hash_algorithm/content_hash` 时，迁移会先补列，再创建 `ix_library_files_dedup_group`。

## 7. Rust 可写字段白名单

### 7.1 scan/hash jobs（`jobs`）

- claim 路径：`status`, `worker_id`, `worker_heartbeat_at`, `lease_expires_at`, `started_at`, `updated_at`
- heartbeat 路径：`processed_items`, `progress`, `worker_heartbeat_at`, `lease_expires_at`, `updated_at`
- finish 路径：`status`, `progress`, `error_code`, `error_message`, `finished_at`, `worker_heartbeat_at`, `lease_expires_at`, `updated_at`

### 7.2 缩略图生成（`thumbnails`）

- claim 路径：`status`, `worker_id`, `worker_heartbeat_at`, `lease_expires_at`, `started_at`, `updated_at`
- heartbeat 路径：`worker_heartbeat_at`, `lease_expires_at`, `updated_at`
- 成功完成路径：`status`, `width`, `height`, `bytes_size`, `error_code`, `error_message`, `error_count`, `retry_after`, `finished_at`, `worker_heartbeat_at`, `lease_expires_at`, `updated_at`
- 失败完成路径：`status`, `error_code`, `error_message`, `error_count`, `retry_after`, `finished_at`, `worker_heartbeat_at`, `lease_expires_at`, `updated_at`

### 7.3 缩略图清理（`thumbnail_cleanup_jobs`）

- claim 路径：`status`, `worker_id`, `worker_heartbeat_at`, `lease_expires_at`, `finished_at`, `updated_at`
- finish 路径：`status`, `error_code`, `error_message`, `finished_at`, `worker_heartbeat_at`, `lease_expires_at`, `updated_at`

### 7.4 全局 I/O 限速（`io_rate_limits`）

- 预算预留路径：对已存在 `bucket_key` 行写入 `next_available_at_ms`, `updated_at`
- 预热路径：当 `bucket_key` 缺失时插入初始化行
- 当前桶标识：`thumbnail_io_global`

### 7.5 WAL 维护（`wal_maintenance_jobs`）

- claim 路径：`status`, `worker_id`, `worker_heartbeat_at`, `lease_expires_at`, `started_at`, `updated_at`, `finished_at`
- stale 回收重试路径：`status`, `retry_count`, `retry_after`, `worker_id`, `worker_heartbeat_at`, `lease_expires_at`, `error_code`, `error_message`, `finished_at`, `updated_at`
- 成功结束路径：`status`, `checkpoint_busy`, `checkpoint_log_frames`, `checkpointed_frames`, `error_code`, `error_message`, `finished_at`, `worker_heartbeat_at`, `lease_expires_at`, `updated_at`
- busy 重试路径：`status`, `retry_count`, `retry_after`, `checkpoint_busy`, `checkpoint_log_frames`, `checkpointed_frames`, `error_code`, `error_message`, `finished_at`, `worker_heartbeat_at`, `lease_expires_at`, `updated_at`
- 失败结束路径：`status`, `error_code`, `error_message`, `finished_at`, `worker_heartbeat_at`, `lease_expires_at`, `updated_at`

Rust 禁止写入：
- 白名单之外的策略字段
- 删除授权或去重语义策略字段
- 任何相似度/启发式重复逻辑

## 8. 缩略图路径与安全契约

- 源媒体路径必须在 `/libraries/*` 下，且通过相对路径校验 + realpath/prefix 校验。
- 缩略图输出只能写入 `/state/thumbs`（或 state 根下配置的等价目录）。
- 清理仅可删除缩略图缓存文件与缩略图索引行。
- 清理绝不能修改 `/libraries` 下原始媒体文件。
