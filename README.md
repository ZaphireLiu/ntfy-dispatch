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

最简单的做法是在 `handlers/` 下新建模块，启动时会自动导入；实际使用的 handler 建议放进独立的私有仓库，见下文。

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
- `env(name, default)`：读取环境变量；
- `publish(topic, message, **fields)`：向 ntfy 发布一条消息，见下文。

handler 必须是同步函数。每个 handler 只有一个 worker 线程，因此 handler 内部状态不需要加锁。

### 发布消息

`ctx.publish()` 用 ntfy 的 [JSON 发布接口](https://docs.ntfy.sh/publish/#publish-as-json) 向当前实例的指定 topic 发一条消息：

```python
@handler(topics=["doorbell"], name="forward_doorbell")
def forward_doorbell(msg: Message, ctx: HandlerContext) -> None:
    ctx.publish(
        "family-phones",
        f"有人按门铃：{msg.message}",
        title="门铃",
        priority=4,
        tags=("bell",),
    )
```

支持的字段与 ntfy 的 JSON 发布一致：`message`、`title`、`priority`（1–5）、`tags`、`actions`、`click`、`attach`、`filename`、`markdown`、`icon`、`delay`、`timezone`、`email`、`call`、`sequence_id`，以及透传未知字段的 `extra`。返回值是 `PublishResult`，带 ntfy 生成的消息 `id`。

发布前会做 topic 校验：

- 发给**该 handler 自己订阅的 topic**：抛 `SelfPublishError`，请求不会发出。
- 发给**本进程其他 handler 订阅的 topic**：记一条 WARNING（每个 handler + topic 只记一次），然后正常发送。
- 发给其他 topic：直接发送。

注意：

- 发布是同步的，会占用当前 handler 的 worker；ntfy 慢会拖慢该 handler 的队列。
- 失败抛 `PublishError`，不会自动重试；需要保证的 handler 自己用重试逻辑，并让下游幂等。
- 发布只做直连检查：A→B→A 这类跨 handler 循环仍可能出现，设计 handler 时要保证发布关系无环。
- 认证沿用 `[ntfy]` 配置，token 需要写权限；`auth = "none"` 时任何人都能向该 topic 发布。

### 把 handler 放进独立的私有仓库

公开仓库只保留框架和示例。实际 handler 往往包含私有 topic、webhook 地址和内网服务名，这些一旦进了公开 git 历史就很难彻底清除，所以建议单独建一个私有仓库，把它作为普通 Python 包装进引擎。

**1. 建仓库和包**

```bash
mkdir ntfy-handlers && cd ntfy-handlers
git init -b main
gh repo create ntfy-handlers --private --source=. --remote=origin
```

目录约定：

```text
ntfy-handlers/
├── pyproject.toml
├── .gitignore          # 至少包含 config.toml、.env、state/、.venv/
├── myhandlers/
│   ├── __init__.py
│   ├── _lib.py         # 共享工具；下划线开头不会被当成 handler 扫描
│   └── forward.py
├── tests/
└── docs/HANDLERS.md    # topic → handler → 环境变量 的总表
```

最小 `pyproject.toml`：

```toml
[build-system]
requires = ["hatchling>=1.24"]
build-backend = "hatchling.build"

[project]
name = "myhandlers"
version = "0.1.0"
requires-python = ">=3.11"
# 引擎还没发布到 PyPI，不要在这里声明 ntfy-dispatch 依赖；
# 两者共用同一个 venv 即可。
dependencies = []

[tool.hatch.build.targets.wheel]
packages = ["myhandlers"]
```

**2. 装进引擎的 venv**

```bash
# 在引擎目录
uv pip install -e .
uv pip install -e ../ntfy-handlers
```

**3. 让引擎加载它**

```toml
[core]
handler_packages = ["myhandlers"]
```

`handlers` 是公开仓库里的示例包，不需要时从列表里去掉。包内以 `_` 开头的模块会被跳过，适合放 `_lib.py` 这样的共享工具；其余模块会被递归导入，所以不要在导入时做耗时或带副作用的事情，除非那就是注册 handler 的装饰器。

**4. 约定**

- 每个 handler 显式写 `name="..."`，格式 `<领域>_<动作>`，全小写、不带点号。它同时是 `[handlers.<name>]` 配置段、logger 名和 stats 键，上线后不要改。
- `[handlers.<name>]` 只放非敏感配置；token 等密钥一律用 `ctx.env("XXX")`。
- 私有仓库的 `docs/HANDLERS.md` 维护 topic 清单和 handler 模板；部署用的 `config.toml` 不进版本库。
- 在私有仓库里记录适配的引擎版本或接口，升级引擎时先跑两边测试，再跑 `--check`。

部署时 systemd 指向的 venv 必须同时装了引擎和私有包；私有包是 editable 安装，更新私有仓库后重启进程即可生效。

### 测试 handler

`ntfy_dispatch.testing` 提供 `make_message()` 和 `FakeContext`，可以不启动进程直接调用 handler：

```python
import httpx

from ntfy_dispatch.testing import FakeContext, make_message

from myhandlers.forward import forward


def test_forward_posts_to_webhook():
    requests = []

    def respond(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(204)

    ctx = FakeContext(
        name="forward",
        config={"target": "https://example.com/hook"},
        http=httpx.Client(transport=httpx.MockTransport(respond)),
    )

    forward(make_message(id="m1", message="hello"), ctx)

    assert len(requests) == 1
```

安装本包后，pytest 会通过 `pytest11` entry point 自动加载 `ntfy_dispatch.pytest_plugin`，在每个测试前后重置全局 registry，因此多个测试文件可以放心导入注册了同名 handler 的模块。

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
