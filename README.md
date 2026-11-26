# ReplayMQ — 支持历史重放的本地消息队列

零依赖（Python 3.11 标准库）的进程内消息队列，在普通实时消费之外提供**历史补账重放**能力。
重放与实时流量共享同一份事件日志，但使用**独立游标、独立资源配额**，并对数据缺口给出明确报告。

## 需求与实现对照

| 需求 | 实现 |
| --- | --- |
| 补账期间新流量正常投递 | 实时游标 `live:*` 与重放游标 `replay:*` 并存；重放边界在发起时固定（`boundary_seq`），边界后的新事件只走实时游标（`broker.py: start_replay/poll`） |
| 重放使用独立游标 | 每个 `ReplaySession` 拥有独立 `Cursor`，各自维护 `next_seq / committed_seq / in-flight`，互不影响 |
| 重放使用独立资源配额 | 每会话独立字节令牌桶（速率 + 突发）+ 总字节配额；实时通道从不经过该闸门（`cursor.py: TokenBucket/ReplaySession.charge`） |
| 查不到最早数据时明确指出缺口 | `TopicLog.gap_watermark` 锚定历史上被清除的最老事件；请求时间落入已清除区间时返回结构化 `Gap`（含时间区间、seq 区间、最老可用事件），会话进入 `GAP` 态，需显式 `acknowledge_gap` |
| 同一事件跨重放边界，页面查看两条记录如何对齐 | 投递账本按 `(topic, event_id)` 归并为 `LedgerEntry`，`alignment_view` / Dashboard 同时展示 LIVE 与 REPLAY 两条记录及各自提交状态 |
| 两个游标并存时区分哪个结果已提交 | `ack(cursor_id, seq)` 必须显式指定游标，返回 `AckResult` 标明归属（`cursor_kind/session_id`）与该游标的 `committed_seq`；乱序 ack 连续提交 |
| 重放占用过高要能降速 | `throttle_replay(rate, drain=True)` 动态降速并可立即排空存量令牌；`pause_replay/resume_replay` 暂停恢复；限流抛 `RateLimited` 带退避毫秒数，实时通道不受影响 |
| 回放覆盖实时失败时间段时保住最老可用事件 | 保留策略 trim 受所有活动游标（含未完成重放）的 `pin_seq` 约束，任何 trim 不得越过最小 pin；会话 FINISHED/FAILED 后 pin 才释放 |

## 目录结构

```
replaymq/
  storage.py    Event / Gap / TopicLog（追加日志、二分时间定位、trim 与 gap 水位）/ JsonlStore
  cursor.py     Cursor（位点、in-flight、乱序 ack、超时重投）/ TokenBucket / ReplaySession
  broker.py     Broker：发布、订阅、重放、poll/ack、保留 pin、对齐账本
  consumer.py   推送式消费者：live / replay 独立工作线程，限流自动退避
  dashboard.py  标准库 http.server 只读页面（对齐表、会话状态、缺口/配额）
tests/          22 个 unittest（语义、线程集成、持久化、HTTP）
demo.py         7 个场景的端到端演示
run_dashboard.py 启动 Web 页面
```

## 快速开始

```bash
python3 -m unittest discover -s tests -v   # 全部测试
python3 demo.py                            # 端到端场景演示
python3 run_dashboard.py                   # http://127.0.0.1:8077 查看对齐页面
```

## 核心语义

### 1. 发起补账

```python
broker = Broker(retention_max_events=100)
live = broker.subscribe("acct-svc", "orders")           # 实时游标，只收新事件
session = broker.start_replay(
    "acct-svc", "orders", from_time_ms=1700000000000,
    rate_bytes_per_sec=10_000, burst_bytes=20_000,       # 独立速率配额
    quota_total_bytes=10_000_000,                         # 独立总配额
)
if session.state == ReplayState.GAP:
    print(session.gap.describe())                        # 明确缺口：时间区间 + seq 区间 + 最老可用事件
    broker.acknowledge_gap(session.session_id)           # 确认后从最老可用事件继续
```

### 2. 两个游标各自投递、各自提交

```python
for env in broker.poll(session.cursor.cursor_id, 10):    # 补账结果，cursor_kind="replay"
    result = broker.ack(env.cursor_id, env.event.seq)    # 必须带 replay 游标 id
    print(result.describe())                             # 标明归属 REPLAY/<session> 与 committed_seq

for env in broker.poll(live.cursor_id, 10):              # 实时结果，cursor_kind="live"
    broker.ack(env.cursor_id, env.event.seq)             # 同 seq 也互不干扰
```

### 3. 缺口报告示例

```
[缺口] topic=orders 无法从 time=0 完整重放：time[10..50) / seq[0..3] 的事件已被保留策略清除；
最老可用事件 -> seq=4 time=50
```

### 4. 降速与保留

```python
broker.throttle_replay(session.session_id, rate_bytes_per_sec=1, drain=True)  # 紧急降速
broker.pause_replay(session.session_id) / broker.resume_replay(...)
# 未 ack 的重放事件会把 trim 钉在最老可用 seq 上，补账完成前不会被清除
```

## 投递语义

- **at-least-once**：`nack` 与可见性超时（`visibility_timeout_ms`）都会重投，`attempts` 累加；业务侧用 `event_id` 幂等
- **乱序 ack**：内部暂存空洞，前序补齐后 `committed_seq` 连续推进
- **持久化（可选）**：`Broker(store_dir=...)` 启用 append-only JSONL，重启后事件日志与 gap 水位（`.meta` sidecar）完整可读；游标/重放会话为运行时态，不跨进程持久化
- **重放完成条件**：固定边界内事件全部提交（`committed_seq >= boundary_seq`）且无 in-flight，会话转 `FINISHED`

## 设计边界

- 单进程、线程安全；不做跨进程消费组协调
- 配额按事件 payload 字节计费（`size_bytes`），令牌桶时钟可注入以便测试
- 缺口只能报告不能回填：被 trim 的数据物理上已不存在，消费者需自行决定从最老可用事件继续或放弃
