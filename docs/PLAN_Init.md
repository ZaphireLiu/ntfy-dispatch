> 本文档记录 ntfy-dispatch 初版（v0.1）的设计与实现计划：常驻进程通过单条 ntfy JSON 流订阅多个 topic，去重后把消息分发给已注册的 handler。
>
> 最后修改：2026-11-24

# ntfy-dispatch 初版设计

## 1. 背景与目标

把 ntfy 当作事件总线：一个常驻 Python 进程维持一条订阅连接，订阅多个 topic；收到消息后，按照 handler 注册的 topic 范围，把消息投递给彼此独立的 handler，由 handler 执行具体动作（调 API、跑命令、改状态等）。

初版目标：

- 单进程、单 ntfy 实例、一条 JSON 流连接订阅多个 topic。
- handler 通过装饰器注册，topic 集合自动推导。
- reader 与 handler 解耦：reader 只负责解析、去重、入队，不执行 handler。
- 每个 handler 一个队列 + 一个 worker 线程，串行消费；handler 之间互不阻塞。
- 持久化去重，避免重连/回放导致重复处理。
- 配置 TOML，密钥只从环境变量读取；支持 `--check` 做启动前校验。

## 2. 已确认的设计决策

| 项目 | 决策 |
| --- | --- |
| 语言 | Python 3.11+ |
| 并发模型 | thread + queue |
| 队列满 | 有界队列，丢新消息，计数 + 限流告警 |
| 去重键 | `(instance, topic, msg_id)` |
| 实例范围 | v1 单实例，表结构保留 `instance` 字段 |
| 崩溃语义 | 内存队列 + 持久去重，at-most-once |
| handler 调度 | 无优先级；handler 间无序，单 handler 内 FIFO |
| ntfy 接入 | 单条 JSON 流，多 topic 逗号拼接 |

## 3. 总体架构

```text
ntfy JSON 流
  └─> reader 线程（每实例一个）
        1. 逐行解析
        2. 计算 key (instance, topic, msg_id)
        3. 查内存 LRU -> 查/写 SQLite
        4. 按 topic 找到匹配的 handlers
        5. 对每个 handler put_nowait
  └─> handler worker 线程（每 handler 一个，FIFO）
        6. 调 handler(msg, ctx)
```

- reader 不做业务、不等待 handler。
- 每个 handler 一个独立队列和一个独立 worker，慢 handler 不会拖住其他 handler。
- reader 唯一可能花费时间的是 SQLite 写入；用 WAL、`busy_timeout` 和一次事务把影响限制在微秒到毫秒级。
- 不保证跨 handler 的执行顺序，也不保证不同 topic 之间的到达顺序；只保证同一 handler 内按 reader 入队顺序 FIFO 消费。

## 4. 消息模型

`Message` 是 ntfy `message` 事件的只读视图，包含：

- `instance`：ntfy 实例名。
- `topic`、`id`、`time`：路由、去重和恢复所需字段。
- `message`、`title`、`priority`、`tags`、`click`、`actions`、`attachment`：原样提供给 handler。
- `raw`：原始事件字典，便于 handler 读取未来新增字段。

`event` 只处理 `message`；`open` 用于确认订阅，`keepalive` 用于判断连接健康，`message_delete` / `message_clear` / `poll_request` 在 v1 中忽略。

## 5. 去重与恢复

### 5.1 去重键

`(instance, topic, msg_id)`。

- 重放时 topic 不变，仍能去重。
- 避免同一个 id 在不同 topic 正常出现时被误杀。

### 5.2 SQLite 结构

```sql
CREATE TABLE IF NOT EXISTS seen (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    instance TEXT NOT NULL,
    topic TEXT NOT NULL,
    msg_id TEXT NOT NULL,
    seen_at INTEGER NOT NULL,
    UNIQUE(instance, topic, msg_id)
);
CREATE INDEX IF NOT EXISTS idx_seen_seen_at ON seen(seen_at);

CREATE TABLE IF NOT EXISTS instance_state (
    instance TEXT PRIMARY KEY,
    last_time INTEGER,
    last_id TEXT,
    updated_at INTEGER
);
```

- `seen_at` 使用本地接收时间，用于清理；恢复点使用 ntfy 消息自带的 `time`。
- 默认保留 7 天，最多 20 万条，由后台 `DedupePruner` 线程每小时清理一次（启动时也会先清理一次）。
- 内存中维护最近 1 万条 key 的 LRU，避免每条消息都访问数据库；清理后清空 LRU，避免已删除的 key 误伤后续回放。

### 5.3 写入顺序与语义

1. reader 先查内存 LRU。
2. 未命中则在一个 SQLite 事务里 `INSERT OR IGNORE`，并更新 `instance_state.last_time`。
3. 标记成功后，再投递到各 handler 队列。

因此整体是 **at-most-once**：

- 进程崩溃会丢失队列中尚未处理的消息。
- 重启后不会重复处理已经标记过的消息。
- 队列满导致丢弃时，消息已经标记，不会被重新投递。
- 数据库写入失败时降级为内存去重并记录错误；重启后可能出现重复。

### 5.4 断线恢复

- 冷启动没有恢复点时，不带 `since`，只收新消息。
- 有恢复点时，重连使用 `since=<last_time - 120s>` 而不是 `since=<last_id>`。
- 原因：ntfy 多 topic 回放时同一秒内跨 topic 的顺序不保证；用时间窗 + 去重重放更稳。
- 响应头带 `X-Messages-Truncated: 1` 时记录告警，说明服务端缓存不足，中间消息不可恢复。

## 6. 队列与背压

- 每个 handler 一个 `queue.Queue`，默认 `maxsize=1000`，可在装饰器中覆盖。
- reader 调用 `put_nowait`；队列满时丢弃新消息，并：
  - 增加该 handler 的 `dropped` 计数；
  - 首次丢弃以及之后每 30 秒记录一次 WARNING，包含消息 id、topic、累计丢弃数和队列深度；
  - 不阻塞 reader，不影响其他 handler。
- 不自动重试失败的 handler。需要重试的 handler 自行实现，并保证幂等。
- handler 抛异常只记录日志，worker 继续消费后续消息。
- handler 必须在合理时间内返回；网络请求应设置超时。线程无法被安全强杀，v1 依赖 handler 自律。

## 7. Handler 注册

```python
@handler(topics=["alerts"], name="forward", queue_size=1000)
def forward(msg: Message, ctx: HandlerContext) -> None:
    ...
```

- 启动时自动导入 `handlers` 包（可配置为其他包），装饰器把 spec 注册到全局 registry。
- 以下情况启动失败：
  - 没有注册任何 handler；
  - handler 名为空或重复；
  - topic 为空或不符合 `[-_A-Za-z0-9]{1,64}`；
  - handler 是 `async def`（v1 只支持同步函数）；
  - topic 并集超过 ntfy 单连接 150 个 topic 的上限。
- `ctx` 提供：
  - `config`：`[handlers.<name>]` 配置段；
  - `logger`：带 handler 名的 logger；
  - `http`：该 handler 独占的懒加载 `httpx.Client`；
  - `instance`：实例名；
  - `shutdown_event`：进程退出信号；
  - `env(name, default)`：读取环境变量，缺失时抛出明确错误。
- 每个 handler 只有一个 worker 线程，因此 handler 内部状态不需要加锁；但不要跨 handler 共享可变全局状态。

## 8. 配置

配置使用 TOML，顶层四段：

```toml
[core]
log_level = "INFO"
state_dir = "state"
handler_packages = ["handlers"]
queue_size = 1000
stats_interval = 300
shutdown_drain_seconds = 10
http_timeout = 10

[ntfy]
name = "default"
base_url = "https://ntfy.sh"
auth = "none" # none | token | basic
# token_env = "NTFY_TOKEN"
# username_env = "NTFY_USER"
# password_env = "NTFY_PASS"
read_timeout = 120
connect_timeout = 10
min_backoff = 1
max_backoff = 60
resume = true
resume_overlap_seconds = 120
tls_verify = true
# ca_file = "ca.pem"

[dedupe]
retention_days = 7
max_rows = 200000
memory_cache = 10000
prune_interval_seconds = 3600

[handlers.example]
# handler 自己的配置
```

- 相对路径（`state_dir`、`ca_file`、dedupe 路径）相对于配置文件所在目录解析。
- 密钥不写入配置文件：`auth = "token"` 时只配置 `token_env`，运行时从环境变量读取；缺失则启动失败。
- `--check` 只加载配置和 handler，打印 handler/topic 列表，不连接 ntfy；缺失密钥只告警。
- 未知的 `[handlers.<name>]` 段给出 WARNING，帮助发现拼写错误。

## 9. ntfy 接入

- URL：`{base_url}/{topics}/json`，topics 为所有 handler topic 的并集，按字典序逗号拼接。
- 认证：
  - access token：`Authorization: Bearer <token>`；
  - basic：`httpx.BasicAuth`；
  - token 和密码只从环境变量读取，不写日志。
- 超时：连接 10s、读取 120s；读取超时应大于 ntfy keepalive 间隔。
- 重连：
  - 指数退避 + jitter，默认 1s 起步，最大 60s；
  - 连接稳定 5 秒以上才重置退避，避免“open 后立即断开”造成忙循环；
  - 429 尊重 `Retry-After`；
  - 401/403 至少退避 300 秒并记录明确的认证错误；
  - 404 至少退避 60 秒并提示检查 base_url / topic。
- 停止时关闭当前 response/client，尽量打断阻塞中的 `iter_lines`；若底层读取没有立即返回，app 最多等待 5 秒，随后让 reader 作为 daemon 线程随进程退出，并跳过 dedupe close 以避免和仍存活的 reader 竞争。

## 10. 日志与可观测性

- 使用标准库 `logging`，输出到 stdout，交给 systemd/journald 收集。
- 默认格式包含时间、级别、线程名、logger 名。
- 核心日志只记录元数据（instance、topic、id、handler），**不记录消息正文**；正文只在 DEBUG 级别由 handler 自己决定是否输出。
- 计数器：
  - 全局：`connects`、`disconnects`、`received`、`deduped`、`enqueued`、`dropped`、`parse_errors`、`db_errors`；
  - 每 handler：`enqueued`、`processed`、`failed`、`dropped`、`queue_depth`。
- 每 `stats_interval` 秒记录一次汇总；退出时记录最终汇总。
- v1 不暴露 HTTP metrics 端点。

## 11. 项目结构

```text
pyproject.toml
README.md
config.example.toml
.gitignore
ntfy_dispatch/
  __init__.py
  __main__.py
  version.py
  cli.py
  app.py
  config.py
  models.py
  registry.py
  dedupe.py
  worker.py
  stream.py
  stats.py
handlers/
  __init__.py
  example.py
tests/
  test_config.py
  test_registry.py
  test_dedupe.py
  test_worker.py
  test_stream.py
  test_integration.py
  test_cli.py
docs/
  PLAN_Init.md
```

## 12. 测试计划

- 单元测试：
  - registry：装饰器注册、重复名、非法 topic、空 topic、async handler、topic 并集、150 上限。
  - dedupe：新消息/重复、跨 topic、重启后仍去重、按时间清理、按条数清理、内存 LRU 淘汰。
  - worker：FIFO、异常隔离、队列满丢弃、停止时排空、handler 上下文。
  - stream：消息解析、URL 与 `since` 参数、重复消息不重复投递。
- 集成测试：
  - 本地假 ntfy HTTP 服务器，发送 `open` + `message`，断开后重连再发一条；
  - 验证消息按序处理、断线后重连带 `since`、优雅退出；
  - 重复消息过滤由 stream 单元测试覆盖。
- 工具：`pytest` + `ruff`。

## 13. 实施顺序

1. 项目骨架、配置、日志、`--check`。
2. registry、handler 自动导入、topic 校验。
3. 去重存储。
4. worker、队列、统计。
5. ntfy reader、重连与恢复。
6. app 组装、信号处理、优雅退出。
7. 示例 handler、README、配置样例。
8. 测试补齐并跑通。

## 14. v1 非目标

- 多 ntfy 实例（表结构保留 `instance`，后续扩展）。
- topic 通配符。
- handler 优先级、过滤、链式调用。
- 持久化队列 / 至少一次 / 自动重试。
- Web UI、Prometheus 端点、热重载。
- 附件自动下载。
- 多进程或分布式消费。

## 15. 风险与后续

- **消息丢失**：at-most-once 是有意取舍；需要可靠投递时应引入 SQLite outbox / 至少一次语义。
- **handler 卡死**：线程无法强杀；依靠超时、队列隔离和退出超时。后续可考虑进程池隔离。
- **停止延迟**：关闭 httpx 流不保证立即唤醒阻塞中的底层读取；最坏情况下 reader 最多占用 5 秒等待，之后随进程退出。若需要更快停止，可缩短 `read_timeout` 或改用可控的底层 socket。
- **重放成本**：`since` 时间窗可能重复回放少量消息；去重会过滤，但会消耗少量带宽。
- **ntfy 缓存限制**：服务端缓存不足时无法补齐停机期间的消息；通过 `X-Messages-Truncated` 告警。
- **SQLite 写延迟**：当前由 reader 同步写；高吞吐场景可改为独立 writer 线程/批量提交。
- **配置错误**：通过 `--check`、启动期 fail-fast 和未知配置段告警降低风险。
