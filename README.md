# ntfy-dispatch

把 [ntfy](https://ntfy.sh) 当作事件总线：一个常驻 Python 进程用一条 JSON 流订阅多个 topic，去重后把消息分发给注册的 handler。

```text
ntfy ──单连接──> reader 线程 ──去重──> 路由 ──┬─> handler A 队列 ─> worker A
                                            ├─> handler B 队列 ─> worker B
                                            └─> handler C 队列 ─> worker C
```

## 特性

- 一个 ntfy 实例一条长连接，订阅所有 handler 声明 topic 的并集。
- handler 用装饰器注册，topic 集合自动推导。
- 每个 handler 一个独立队列 + 独立 worker，慢 handler 不阻塞其他 handler。
- SQLite 持久化去重，重连/回放不会重复处理。
- 断线指数退避重连，并按时间窗使用 `since` 补收消息。
- 队列满时丢弃新消息并告警，reader 不等待 handler。
- TOML 配置，密钥只从环境变量读取。
- `--check` 启动前校验配置和 handler。

## 要求

- Python 3.11+
- 运行依赖：`httpx`

## 安装

```bash
python -m venv .venv
. .venv/bin/activate
pip install -e ".[dev]"
```

## 配置

复制示例配置：

```bash
cp config.example.toml config.toml
```

最小配置：

```toml
[core]
handler_packages = ["handlers"]

[ntfy]
base_url = "https://ntfy.sh"
auth = "none"
```

需要认证时，把 token 放进环境变量，而不是配置文件：

```toml
[ntfy]
auth = "token"
token_env = "NTFY_TOKEN"
```

```bash
export NTFY_TOKEN="..."
```

`config.toml`、`.env` 和 `state/` 已在 `.gitignore` 中排除。

## 运行

```bash
# 只校验配置和 handler，不连接 ntfy
ntfy-dispatch --config config.toml --check

# 启动
ntfy-dispatch --config config.toml
```

也可以：

```bash
python -m ntfy_dispatch --config config.toml
```

收到 `SIGINT` / `SIGTERM` 后会停止 reader，并在 `shutdown_drain_seconds` 内尽量排空 handler 队列。

## 写 handler

在 `handlers/` 下新建模块，启动时会自动导入：

```python
from ntfy_dispatch import HandlerContext, Message, handler


@handler(topics=["alerts"], name="forward", queue_size=1000)
def forward(msg: Message, ctx: HandlerContext) -> None:
    ctx.logger.info("got %s from %s", msg.id, msg.topic)

    target = ctx.config.get("target")
    if target:
        response = ctx.http.post(
            str(target),
            json={"id": msg.id, "topic": msg.topic, "message": msg.message},
        )
        response.raise_for_status()
```

对应的配置：

```toml
[handlers.forward]
target = "https://example.com/hook"
```

`ctx` 提供：

- `config`：`[handlers.<name>]` 段；
- `logger`：带 handler 名的 logger；
- `http`：该 handler 独占的懒加载 `httpx.Client`；
- `instance`：ntfy 实例名；
- `shutdown_event`：进程退出信号；
- `env(name, default)`：读取环境变量。

handler 必须是同步函数。每个 handler 只有一个 worker 线程，因此 handler 内部状态不需要加锁。

## 语义与注意事项

- **at-most-once**：消息先写去重记录再入队；进程崩溃会丢失队列中未处理的消息，但重启后不会重复处理已标记的消息。
- **单 handler 内 FIFO**：同一 handler 按 reader 入队顺序消费；handler 之间、不同 topic 之间不保证顺序。
- **队列满丢新**：每个 handler 队列默认 1000，满了丢新消息并记录 WARNING；其他 handler 不受影响。
- **不自动重试**：handler 失败只记录日志。需要重试的 handler 自己实现，并保证幂等。
- **不记录正文**：核心日志只记录消息元数据，正文是否输出由 handler 决定。
- **topic 名可能敏感**：ntfy 的 topic 名相当于密码；核心日志会记录 topic 名用于排障，但不会记录消息正文，也不会让 httpx 打印完整订阅 URL。若 topic 名本身敏感，请调整日志级别或修改日志字段。
- **单实例**：v1 只支持一个 ntfy 实例；SQLite 表已保留 `instance` 字段。
- **精确 topic**：v1 不支持通配符。
- **退出**：停止时最多等待 reader 5 秒；若底层读取没有立即返回，进程会退出，未关闭的 SQLite WAL 会在下次启动时恢复。

## 开发

```bash
pytest
ruff check .
ruff format --check .
```

设计细节见 [docs/PLAN_Init.md](docs/PLAN_Init.md)。

## systemd 示例

```ini
[Unit]
Description=ntfy-dispatch
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=ntfy
WorkingDirectory=/opt/ntfy-dispatch
EnvironmentFile=-/etc/ntfy-dispatch.env
ExecStart=/opt/ntfy-dispatch/.venv/bin/ntfy-dispatch --config /etc/ntfy-dispatch/config.toml
Restart=on-failure
RestartSec=5

NoNewPrivileges=yes
PrivateTmp=yes
ProtectSystem=strict
ProtectHome=yes
ReadWritePaths=/var/lib/ntfy-dispatch

[Install]
WantedBy=multi-user.target
```

使用 systemd 时，把配置里的 `state_dir` 指向 `/var/lib/ntfy-dispatch`。
