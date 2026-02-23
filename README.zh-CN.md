# DedupFS

[English](./README.md) | [简体中文](./README.zh-CN.md)

> 警告
> 本项目尚未经过真实生产环境测试验证。
> 请勿部署到生产环境。

基于 Python 控制平面 + Rust 数据平面的确定性 NAS 去重系统。

![Python](https://img.shields.io/badge/Python-3.12%2B-3776AB?logo=python&logoColor=white)
![Rust](https://img.shields.io/badge/Rust-stable-000000?logo=rust)
![FastAPI](https://img.shields.io/badge/FastAPI-API-009688?logo=fastapi&logoColor=white)
![SQLite](https://img.shields.io/badge/SQLite-WAL-003B57?logo=sqlite&logoColor=white)
![Stage](https://img.shields.io/badge/Stage-Active%20Development-orange)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)

## 运行架构

- Python API：
  - 策略与校验边界
  - Job 与缩略图队列准入
  - 状态机与 stale-lease 恢复
- Rust Worker：
  - scan/hash 执行
  - 缩略图生成与清理执行
  - 租约续期与 I/O 预算预留
- 协议：
  - 仅使用 DB-first 契约（`jobs`、`thumbnails`、`thumbnail_cleanup_jobs`、`library_files`）

## 快速开始

### 1) 启动 Python API

```bash
pip install -e .
DEDUPFS_LIBRARIES_ROOT=/libraries DEDUPFS_STATE_ROOT=/state uvicorn dedupfs.main:app --host 0.0.0.0 --port 8080
```

### 2) 启动 Rust Worker

```bash
cd rust-worker
cargo run -- --worker-id rust-worker-1 --daemon
```

## 常用接口

- Job：`POST /api/v1/jobs`
- 重复组：
  - `GET /api/v1/duplicates/groups`
  - `GET /api/v1/duplicates/groups/{group_key}/files`
- 缩略图：
  - `POST /api/v1/thumbs/request`
  - `GET /api/v1/thumbs/{thumb_key}`
  - `GET /api/v1/thumbs/{thumb_key}/content`
  - `GET /api/v1/thumbs/metrics`
- WAL 维护：
  - `POST /api/v1/maintenance/wal/checkpoint`
  - `GET /api/v1/maintenance/wal/checkpoint/latest`
  - `GET /api/v1/maintenance/wal/metrics`

## 性能基准脚本

- 重复组查询基准：
  - `python scripts/benchmark_duplicates.py --state-root /tmp/dedupfs-bench --groups 5000 --files-per-group 2 --page-size 200 --explain`
- 高基数 UI 参数分析：
  - `python scripts/profile_ui_duplicates.py --state-root /tmp/dedupfs-ui-profile --groups 20000 --files-per-group 2 --page-size-candidates 120,160,200 --overscan-candidates 4,6,8 --viewport-heights 720,900,1080 --group-row-height 84 --target-max-dom-rows 24 --report-json /tmp/dedupfs-ui-profile/report.json`
- 缩略图队列吞吐/争用基准：
  - `python scripts/benchmark_thumbnail_queue.py --state-root /tmp/dedupfs-thumb-bench --files 5000 --requests 12000 --workers 12 --hot-file-count 800 --queue-capacity 50000 --min-throughput-rps 250 --max-failed-ratio 0.001 --max-queue-full-ratio 0.01 --min-dedupe-ratio 0.85`

## 相关文档

- 协议：`docs/PROTOCOL.md` / `docs/PROTOCOL.zh-CN.md`
- 架构：`docs/ARCHITECTURE.md` / `docs/ARCHITECTURE.zh-CN.md`

## License

Apache License 2.0。详见 `LICENSE`。
