"""Broker：队列入口，编排日志、游标、重放会话、保留策略与对齐账本。

线程模型：所有公共方法线程安全；TopicLog / Cursor / ReplaySession 各自带锁，
Broker 上一把大锁只保护注册表与账本的一致性，不在锁内做配额等待。
"""

from __future__ import annotations

import itertools
import threading
import time
from dataclasses import dataclass, field
from typing import Optional

from .storage import Event, Gap, TopicLog, JsonlStore
from .cursor import Cursor, ReplaySession, ReplayState, RateLimited, clock_ms


@dataclass
class Envelope:
    """一次投递的包装；同一条 Event 可在 live / replay 两个游标各产生一个 Envelope。"""

    event: Event
    cursor_id: str
    cursor_kind: str          # "live" | "replay" —— 明确这条结果属于哪个游标
    session_id: Optional[str]
    attempts: int

    @property
    def committed_marker(self) -> str:
        """供页面/日志区分来源的标签。"""
        if self.cursor_kind == "live":
            return f"LIVE({self.cursor_id})"
        return f"REPLAY({self.session_id})"


@dataclass
class AckResult:
    """ack 的归属凭证：两个游标并存时，靠它区分哪个结果已提交。"""

    cursor_id: str
    cursor_kind: str
    session_id: Optional[str]
    seq: int
    event_id: str
    committed_advanced: bool
    committed_seq: int

    def describe(self) -> str:
        owner = self.cursor_kind.upper()
        sess = f"/{self.session_id}" if self.session_id else ""
        flag = "已提交" if self.committed_advanced else "已确认(等待前序补齐)"
        return (
            f"event={self.event_id} seq={self.seq} 归属游标={owner}{sess} "
            f"{flag}，该游标 committed_seq={self.committed_seq}"
        )


@dataclass
class DeliveryRecord:
    """账本中的一条投递记录。"""

    cursor_id: str
    cursor_kind: str
    session_id: Optional[str]
    seq: int
    attempts: int
    delivered_at_ms: int
    committed: bool = False
    committed_at_ms: Optional[int] = None


@dataclass
class LedgerEntry:
    """同一事件在不同游标上的全部记录 —— 页面的实时/补账对齐视图就基于它。"""

    event: Event
    records: list[DeliveryRecord] = field(default_factory=list)

    def alignment(self) -> dict:
        live = [r for r in self.records if r.cursor_kind == "live"]
        replay = [r for r in self.records if r.cursor_kind == "replay"]
        return {
            "event_id": self.event.event_id,
            "seq": self.event.seq,
            "event_time": self.event.event_time,
            "live": [
                {"cursor_id": r.cursor_id, "committed": r.committed, "committed_at_ms": r.committed_at_ms}
                for r in live
            ],
            "replay": [
                {
                    "session_id": r.session_id,
                    "cursor_id": r.cursor_id,
                    "committed": r.committed,
                    "committed_at_ms": r.committed_at_ms,
                }
                for r in replay
            ],
        }


class Broker:
    def __init__(
        self,
        retention_max_events: Optional[int] = None,
        retention_max_age_ms: Optional[int] = None,
        visibility_timeout_ms: int = 30_000,
        store_dir: Optional[str] = None,
        clock=None,
    ):
        self._clock = clock or time.monotonic
        self.retention_max_events = retention_max_events
        self.retention_max_age_ms = retention_max_age_ms
        self.visibility_timeout_ms = visibility_timeout_ms

        self._topics: dict[str, TopicLog] = {}
        self._stores: dict[str, JsonlStore] = {}
        self._store_dir = store_dir
        self._live_cursors: dict[tuple[str, str], Cursor] = {}
        self._cursor_owner: dict[str, Cursor] = {}
        self._sessions: dict[str, ReplaySession] = {}
        self._ledger: dict[tuple[str, str], LedgerEntry] = {}
        self._seq = itertools.count(1)
        self._lock = threading.RLock()

        if store_dir is not None:
            import os
            os.makedirs(store_dir, exist_ok=True)
            for name in os.listdir(store_dir):
                if name.endswith(".jsonl"):
                    topic = name[: -len(".jsonl")]
                    self._stores[topic] = JsonlStore(f"{store_dir}/{name}")
                    self._topics[topic] = TopicLog(topic, self._stores[topic])

    # ================= 日志与发布 =================

    def _topic(self, topic: str) -> TopicLog:
        log = self._topics.get(topic)
        if log is None:
            store = None
            if self._store_dir is not None:
                store = JsonlStore(f"{self._store_dir}/{topic}.jsonl")
                self._stores[topic] = store
            log = TopicLog(topic, store)
            self._topics[topic] = log
        return log

    def publish(self, topic: str, event_id: str, payload: dict) -> Event:
        """新流量入口。重放进行中完全不受影响：实时游标与重放配额相互独立。"""
        with self._lock:
            log = self._topic(topic)
            event = log.append(event_id, clock_ms(self._clock), payload)
            self._ledger[(topic, event_id)] = LedgerEntry(event=event)
            self._apply_retention(topic)
            return event

    # ================= 实时订阅 =================

    def subscribe(self, consumer: str, topic: str) -> Cursor:
        """创建实时游标，从当前日志尾部开始 —— 只收新流量。"""
        with self._lock:
            key = (topic, consumer)
            existing = self._live_cursors.get(key)
            if existing is not None:
                return existing
            log = self._topic(topic)
            cursor = Cursor(f"live:{consumer}:{topic}", "live", topic, log.hi_seq + 1)
            self._live_cursors[key] = cursor
            self._cursor_owner[cursor.cursor_id] = cursor
            return cursor

    # ================= 历史重放 =================

    def start_replay(
        self,
        consumer: str,
        topic: str,
        from_time_ms: int,
        rate_bytes_per_sec: float = 10_000,
        burst_bytes: int = 20_000,
        quota_total_bytes: int = 10_000_000,
    ) -> ReplaySession:
        """从指定时间发起补账重放。

        - 起始时间早于最老可用事件：会话进入 GAP 状态并携带结构化缺口报告，
          游标仍定位在最老可用事件上，消费者确认缺口后可从该处继续。
        - 起始时间晚于所有事件：返回立即 FINISHED 的空重放（不算缺口）。
        - 重放边界 = 发起时刻日志的 hi_seq；边界之后的新事件只走实时游标。
        """
        with self._lock:
            log = self._topic(topic)
            session_id = f"replay-{next(self._seq)}"
            cursor_id = f"replay:{consumer}:{session_id}"
            oldest = log.oldest()
            target = log.find_first_at_or_after(from_time_ms)

            # 缺口判定：请求起点早于"最老可用事件时间"且日志确实发生过清除。
            # 注意请求时间 >= oldest.event_time 时缺口区间与请求区间不重叠，
            # 即使日志带 gap_anchor 也是正常重放，不报缺口。
            gap: Optional[Gap] = None
            if oldest is None:
                if log.gap_watermark is not None:
                # 日志曾被 trim 清空：任何历史请求都只能得到缺口报告
                    gap = log.build_gap(from_time_ms)
                    start_seq = log.hi_seq + 1
                else:
                    # 从未有过数据：不存在"已丢失"事件，空重放立即结束
                    start_seq = log.hi_seq + 1
            elif from_time_ms < oldest.event_time and log.gap_watermark is not None:
                gap = log.build_gap(from_time_ms)
                start_seq = target.seq if target is not None else oldest.seq
            elif target is None:
                # 没有满足时间要求的事件且无缺口：空重放（请求的是未来时间），立即结束
                start_seq = log.hi_seq + 1
            else:
                start_seq = target.seq

            cursor = Cursor(cursor_id, "replay", topic, start_seq)
            self._cursor_owner[cursor_id] = cursor
            session = ReplaySession(
                session_id=session_id,
                consumer=consumer,
                cursor=cursor,
                from_time=from_time_ms,
                rate_bytes_per_sec=rate_bytes_per_sec,
                burst_bytes=burst_bytes,
                quota_total_bytes=quota_total_bytes,
                clock=self._clock,
                gap=gap,
            )
            session.boundary_seq = log.hi_seq  # type: ignore[attr-defined]
            # 请求时间晚于所有事件：空重放，创建即结束（区别于缺口）
            if gap is None and start_seq > log.hi_seq:
                session.finish(self._clock)
            self._sessions[session_id] = session
            return session

    def acknowledge_gap(self, session_id: str, continue_from_oldest: bool = True) -> ReplaySession:
        """消费者明确知悉缺口后的处理：确认则从最老可用事件继续重放。"""
        with self._lock:
            session = self._sessions[session_id]
            if session.state != ReplayState.GAP:
                raise IllegalStateException(f"会话 {session_id} 不在 GAP 状态（当前 {session.state.value}）")
            if not continue_from_oldest:
                session.state = ReplayState.FAILED
                session.finished_at_ms = clock_ms(self._clock)
            else:
                session.state = ReplayState.RUNNING
            return session

    # ================= 投递与确认 =================

    def poll(self, cursor_id: str, max_events: int = 10) -> list[Envelope]:
        """从指定游标拉取事件。

        replay 游标逐条过独立配额闸门，配额不足立即抛 RateLimited（带退避建议），
        已通过闸门的事件照常返回 —— 降速只影响重放，实时游标永远不走该闸门。
        """
        with self._lock:
            cursor = self._cursor_owner.get(cursor_id)
            if cursor is None:
                raise unknown_cursor(cursor_id)
            log = self._topics[cursor.topic]

            # 超时重投（at-least-once）：reclaim 已回退 next_seq 且保留 attempts 计数
            cursor.reclaim_timeouts(clock_ms(self._clock), self.visibility_timeout_ms)

            available = log.slice_from(cursor.next_seq)
            envelopes: list[Envelope] = []
            session = self._find_session(cursor_id)
            throttled: Optional[RateLimited] = None

            for event in available:
                if len(envelopes) >= max_events:
                    break
                if session is not None:
                    # 重放边界之外的事件不属于本次补账
                    if event.seq > session.boundary_seq:  # type: ignore[attr-defined]
                        break
                    try:
                        session.charge(event.size_bytes)  # 配额/速率/暂停闸门
                    except RateLimited as exc:
                        # 降速只拦住后续事件；已通过闸门的事件照常返回，不丢投递
                        throttled = exc
                        break
                cursor.claim(event, clock_ms(self._clock))
                envelopes.append(
                    Envelope(
                        event=event,
                        cursor_id=cursor.cursor_id,
                        cursor_kind=cursor.kind,
                        session_id=session.session_id if session else None,
                        attempts=cursor.in_flight[event.seq].attempts,
                    )
                )
                self._record_delivery(cursor, session, event)

            self._maybe_finish_replay(session)
            if throttled is not None and not envelopes:
                # 一条都没取到才抛出，调用方按 retry_after_ms 退避
                raise throttled
            return envelopes

    def ack(self, cursor_id: str, seq: int) -> AckResult:
        """确认事件。必须显式带 cursor_id —— 两个游标并存时以此区分提交归属。"""
        with self._lock:
            cursor = self._cursor_owner.get(cursor_id)
            if cursor is None:
                raise unknown_cursor(cursor_id)
            advanced, committed_seq = cursor.ack(seq)
            session = self._find_session(cursor_id)

            entry = self._find_ledger_entry(cursor.topic, seq)
            if entry is not None:
                for record in entry.records:
                    if record.cursor_id == cursor_id and record.seq == seq:
                        record.committed = True
                        record.committed_at_ms = clock_ms(self._clock)

            self._maybe_finish_replay(session)
            self._apply_retention(cursor.topic)
            return AckResult(
                cursor_id=cursor.cursor_id,
                cursor_kind=cursor.kind,
                session_id=session.session_id if session else None,
                seq=seq,
                event_id=entry.event.event_id if entry else f"seq:{seq}",
                committed_advanced=advanced,
                committed_seq=committed_seq,
            )

    def nack(self, cursor_id: str, seq: int) -> None:
        with self._lock:
            cursor = self._cursor_owner.get(cursor_id)
            if cursor is None:
                raise unknown_cursor(cursor_id)
            cursor.nack(seq)

    # ================= 重放速率/配额管控 =================

    def throttle_replay(self, session_id: str, rate_bytes_per_sec: float, drain: bool = False) -> None:
        """重放占用过高时动态降速（0 等效暂停令牌发放）。

        drain=True 排空桶内存量令牌，降速立即生效；否则存量令牌仍可继续消费。
        """
        self._sessions[session_id].set_rate(rate_bytes_per_sec, drain=drain)

    def pause_replay(self, session_id: str) -> None:
        self._sessions[session_id].pause()

    def resume_replay(self, session_id: str) -> None:
        self._sessions[session_id].resume()

    # ================= 保留策略：保住最老可用事件 =================

    def _replay_pins(self) -> dict[str, int]:
        pins: dict[str, int] = {}
        for session in self._sessions.values():
            if session.state in (ReplayState.FINISHED, ReplayState.FAILED):
                continue  # 结束的重放不再钉住日志
            pins.setdefault(session.cursor.topic, session.cursor.pin_seq())
            pins[session.cursor.topic] = min(pins[session.cursor.topic], session.cursor.pin_seq())
        return pins

    def _apply_retention(self, topic: str) -> int:
        log = self._topics[topic]
        if not len(log):
            return 0
        pins = [c.pin_seq() for (t, _), c in self._live_cursors.items() if t == topic]
        pins += [p for t, p in self._replay_pins().items() if t == topic]
        min_pin = min(pins) if pins else log.hi_seq + 1  # 没有任何游标时允许全清

        trim_to = log.lo_seq
        if self.retention_max_events is not None:
            want = log.hi_seq + 1 - self.retention_max_events
            trim_to = max(trim_to, want)
        if self.retention_max_age_ms is not None:
            threshold = clock_ms(self._clock) - self.retention_max_age_ms
            first_fresh = log.find_first_at_or_after(threshold)
            if first_fresh is not None:
                trim_to = max(trim_to, first_fresh.seq)
        # 关键约束：任何 trim 都不能越过最老的活动游标 pin
        trim_to = min(trim_to, min_pin)
        if trim_to > log.lo_seq:
            log.trim_before(trim_to)
        return trim_to

    # ================= 观测 / 对齐视图 =================

    def alignment_view(self, topic: str) -> list[dict]:
        """页面用：按事件顺序给出实时/补账两条记录的对齐情况。"""
        with self._lock:
            log = self._topics.get(topic)
            if log is None:
                return []
            entries = []
            for e in log.iter_all():
                # 持久化重启后账本可能尚未惰性建条目，这里兜底补建
                entry = self._ledger.get((topic, e.event_id))
                if entry is None:
                    entry = LedgerEntry(event=e)
                    self._ledger[(topic, e.event_id)] = entry
                entries.append(entry)
            return [e.alignment() for e in entries]

    def replay_sessions(self, consumer: Optional[str] = None) -> list[dict]:
        with self._lock:
            return [
                s.snapshot()
                for s in self._sessions.values()
                if consumer is None or s.consumer == consumer
            ]

    def cursors(self, topic: Optional[str] = None) -> list[dict]:
        with self._lock:
            return [
                c.snapshot()
                for c in self._cursor_owner.values()
                if topic is None or c.topic == topic
            ]

    def topic_stats(self, topic: str) -> dict:
        with self._lock:
            log = self._topics.get(topic)
            if log is None:
                return {"topic": topic, "events": 0}
            gap = log.gap_watermark
            return {
                "topic": topic,
                "events": len(log),
                "lo_seq": log.lo_seq,
                "hi_seq": log.hi_seq,
                "oldest_event_time": log.oldest().event_time if log.oldest() else None,
                "gap_anchor": None if gap is None else {"seq": gap.seq, "event_time": gap.event_time},
            }

    # ================= 内部工具 =================

    def _find_session(self, cursor_id: str) -> Optional[ReplaySession]:
        for session in self._sessions.values():
            if session.cursor.cursor_id == cursor_id:
                return session
        return None

    def _find_ledger_entry(self, topic: str, seq: int) -> Optional[LedgerEntry]:
        for entry in self._ledger.values():
            if entry.event.topic == topic and entry.event.seq == seq:
                return entry
        return None

    def _record_delivery(self, cursor: Cursor, session: Optional[ReplaySession], event: Event) -> None:
        entry = self._ledger.get((cursor.topic, event.event_id))
        if entry is None:  # 持久化恢复后账本可能缺失，惰性补建
            entry = LedgerEntry(event=event)
            self._ledger[(cursor.topic, event.event_id)] = entry
        entry.records.append(
            DeliveryRecord(
                cursor_id=cursor.cursor_id,
                cursor_kind=cursor.kind,
                session_id=session.session_id if session else None,
                seq=event.seq,
                attempts=cursor.in_flight[event.seq].attempts,
                delivered_at_ms=clock_ms(self._clock),
            )
        )

    def _maybe_finish_replay(self, session: Optional[ReplaySession]) -> None:
        if session is None or session.state not in (ReplayState.RUNNING, ReplayState.PAUSED):
            return
        cursor = session.cursor
        boundary = session.boundary_seq  # type: ignore[attr-defined]
        # 边界在发起时固定：边界内事件全部确认提交（committed_seq 到达边界）即补账完成；
        # 仅投递未 ack 不算完成，之后到达的新事件只属于实时游标。
        # 缺口场景下游标起点可能 > 0，committed_seq == boundary 即视为边界内已清。
        if cursor.committed_seq >= boundary and not cursor.in_flight:
            session.finish(self._clock)


class IllegalStateException(RuntimeError):
    pass


def unknown_cursor(cursor_id: str) -> IllegalStateException:
    return IllegalStateException(f"未知游标: {cursor_id}（ack/poll 必须携带已注册的 cursor_id）")
