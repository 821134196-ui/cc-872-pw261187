"""replaymq — 本地消息队列，支持历史重放（独立游标 / 独立配额 / 缺口报告）。

核心抽象：
    Broker       队列入口：发布事件、创建实时游标、发起重放、trim 保留策略
    TopicLog     单 topic 的追加日志（内存 + 可选 JSONL 持久化），带 gap 水位
    Cursor       消费游标：live 或 replay，各自维护 in-flight 与已提交位点
    ReplaySession 重放会话：独立 token bucket、速率调节、生命周期状态
    Consumer     推送式消费者：live / replay 两条独立工作线程
    Gap          历史缺口：请求时间早于最老可用事件时返回

典型用法见 demo.py，语义测试见 tests/。
"""

from .storage import Event, Gap, TopicLog, JsonlStore
from .cursor import Cursor, ReplaySession, ReplayState
from .broker import Broker, AckResult, Envelope, RateLimited
from .consumer import Consumer

__all__ = [
    "Broker",
    "TopicLog",
    "Event",
    "Gap",
    "Cursor",
    "ReplaySession",
    "ReplayState",
    "AckResult",
    "Envelope",
    "RateLimited",
    "JsonlStore",
    "Consumer",
]
