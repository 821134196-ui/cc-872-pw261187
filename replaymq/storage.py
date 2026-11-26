"""追加式事件日志与持久化。

TopicLog 是单 topic 的有序事件日志：
- 事件按单调递增 seq 追加，携带毫秒时间戳 event_time（发布时由 broker 时钟打戳）
- 物理上保留 [lo_seq, hi_seq] 区间；trim 只能从最老端删除
- gap_watermark 记录本日志历史上被删过的最老事件：
  任何早于 gap_watermark.event_time 的重放请求都无法完整满足，必须明确报缺口
"""

from __future__ import annotations

import json
import os
import threading
from dataclasses import dataclass, asdict
from typing import Iterator, Optional


@dataclass(frozen=True)
class Event:
    """一条不可变事件。seq 在单个 topic 内单调递增（从 0 起）。"""

    seq: int
    event_id: str          # 业务幂等 ID，跨重放边界对齐时用它识别"同一事件"
    topic: str
    event_time: int        # 毫秒时间戳
    payload: dict
    size_bytes: int = 0    # 用于配额计费（payload 序列化后的字节数）

    def to_json(self) -> str:
        return json.dumps(asdict(self), ensure_ascii=False)

    @staticmethod
    def from_json(line: str) -> "Event":
        d = json.loads(line)
        return Event(**d)


@dataclass(frozen=True)
class Gap:
    """历史缺口报告。

    requested_from_time: 消费者要求补账的起始时间
    oldest_available:    当前日志里最老可用事件（若日志已空则为 None）
    gap_from_time:       缺口区间起点（上次 trim 掉的最老事件时间）
    gap_to_time:         缺口区间终点（最老可用事件的前一刻）
    lost_from_seq:       已丢失区间的 seq 范围（含端点），None 表示无法确定
    lost_to_seq:
    """

    topic: str
    requested_from_time: int
    oldest_available: Optional[Event]
    gap_from_time: int
    gap_to_time: int
    lost_from_seq: Optional[int]
    lost_to_seq: Optional[int]

    def describe(self) -> str:
        rng = (
            f"seq[{self.lost_from_seq}..{self.lost_to_seq}]"
            if self.lost_from_seq is not None
            else "seq 范围未知"
        )
        oldest = (
            f"seq={self.oldest_available.seq} time={self.oldest_available.event_time}"
            if self.oldest_available is not None
            else "日志已空，没有任何可用事件"
        )
        return (
            f"[缺口] topic={self.topic} 无法从 time={self.requested_from_time} 完整重放："
            f"time[{self.gap_from_time}..{self.gap_to_time}) / {rng} 的事件已被保留策略清除；"
            f"最老可用事件 -> {oldest}"
        )


class JsonlStore:
    """append-only JSONL 持久化，每行一个 Event。

    只负责追加与全量读取；trim 后的物理压缩由 compact() 完成。
    另外写一个 .meta sidecar 持久化 gap_anchor，保证重启后缺口语义不丢失。
    与 TopicLog 解耦，传 store=None 即为纯内存队列。
    """

    def __init__(self, path: str):
        self.path = path
        self.meta_path = path + ".meta"
        os.makedirs(os.path.dirname(os.path.abspath(path)) or ".", exist_ok=True)
        self._lock = threading.Lock()

    def append(self, event: Event) -> None:
        with self._lock:
            with open(self.path, "a", encoding="utf-8") as f:
                f.write(event.to_json() + "\n")
                f.flush()
                os.fsync(f.fileno())

    def read_all(self) -> list[Event]:
        if not os.path.exists(self.path):
            return []
        with self._lock:
            events: list[Event] = []
            with open(self.path, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if line:
                        events.append(Event.from_json(line))
            return events

    def write_gap_anchor(self, anchor: Optional[Event]) -> None:
        with self._lock:
            if anchor is None:
                if os.path.exists(self.meta_path):
                    os.remove(self.meta_path)
                return
            tmp = self.meta_path + ".tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                f.write(anchor.to_json() + "\n")
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp, self.meta_path)

    def read_gap_anchor(self) -> Optional[Event]:
        if not os.path.exists(self.meta_path):
            return None
        with self._lock:
            with open(self.meta_path, "r", encoding="utf-8") as f:
                line = f.readline().strip()
            return Event.from_json(line) if line else None

    def compact(self, events: list[Event]) -> None:
        """用当前仍保留的事件全量重写文件（trim 后调用）。"""
        tmp = self.path + ".tmp"
        with self._lock:
            with open(tmp, "w", encoding="utf-8") as f:
                for e in events:
                    f.write(e.to_json() + "\n")
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp, self.path)


class TopicLog:
    """线程安全的追加日志。

    不变量：
      1. events 按 seq 升序，且在 lo_seq..hi_seq 上连续（trim 只删前缀）
      2. gap_watermark 记录最后一次 trim 删掉的最老事件；从未 trim 过为 None
      3. 已 trim 的 seq 永不复用（next_seq 继续单调递增）
    """

    def __init__(self, topic: str, store: Optional[JsonlStore] = None):
        self.topic = topic
        self._store = store
        self._events: list[Event] = []
        self._lock = threading.RLock()
        self._next_seq = 0
        # 历史上被 trim 掉的最老事件（用于报告缺口区间起点）
        self._gap_anchor: Optional[Event] = None

        if store is not None:
            for e in store.read_all():
                self._events.append(e)
                self._next_seq = max(self._next_seq, e.seq + 1)
            self._gap_anchor = store.read_gap_anchor()

    # ---------- 基本属性 ----------

    @property
    def lo_seq(self) -> int:
        with self._lock:
            return self._events[0].seq if self._events else self._next_seq

    @property
    def hi_seq(self) -> int:
        """最后一条事件的 seq；空日志为 lo_seq - 1。"""
        with self._lock:
            return self._events[-1].seq if self._events else self._next_seq - 1

    @property
    def gap_watermark(self) -> Optional[Event]:
        with self._lock:
            return self._gap_anchor

    def __len__(self) -> int:
        with self._lock:
            return len(self._events)

    # ---------- 写入 ----------

    def append(self, event_id: str, event_time: int, payload: dict) -> Event:
        with self._lock:
            size = len(json.dumps(payload, ensure_ascii=False).encode("utf-8"))
            event = Event(
                seq=self._next_seq,
                event_id=event_id,
                topic=self.topic,
                event_time=event_time,
                payload=payload,
                size_bytes=size,
            )
            self._next_seq += 1
            self._events.append(event)
            if self._store is not None:
                self._store.append(event)
            return event

    # ---------- 读取 ----------

    def get(self, seq: int) -> Optional[Event]:
        with self._lock:
            if not self._events or seq < self.lo_seq or seq > self.hi_seq:
                return None
            return self._events[seq - self._events[0].seq]

    def oldest(self) -> Optional[Event]:
        with self._lock:
            return self._events[0] if self._events else None

    def slice_from(self, seq: int) -> list[Event]:
        """返回 seq >= 参数的全部保留事件（拷贝，避免外部突变）。"""
        with self._lock:
            if not self._events:
                return []
            idx = max(0, seq - self._events[0].seq)
            return list(self._events[idx:])

    def iter_all(self) -> Iterator[Event]:
        with self._lock:
            yield from list(self._events)

    def find_first_at_or_after(self, event_time: int) -> Optional[Event]:
        """二分查找 event_time >= 参数的第一条事件。"""
        with self._lock:
            lo, hi = 0, len(self._events)
            while lo < hi:
                mid = (lo + hi) // 2
                if self._events[mid].event_time < event_time:
                    lo = mid + 1
                else:
                    hi = mid
            return self._events[lo] if lo < len(self._events) else None

    # ---------- 保留策略 ----------

    def trim_before(self, seq: int) -> list[Event]:
        """物理删除 seq < 参数的前缀事件，并更新 gap 水位。

        gap_anchor 记录"历史上被清除的最老事件"（首次 trim 的第一条），
        多次 trim 不覆盖，从而缺口区间能完整覆盖全部丢失数据。
        返回被删除的事件。seq <= lo_seq 时为空操作。
        """
        with self._lock:
            if not self._events or seq <= self._events[0].seq:
                return []
            idx = seq - self._events[0].seq
            removed = self._events[:idx]
            if self._gap_anchor is None:
                self._gap_anchor = removed[0]
            self._events = self._events[idx:]
            if self._store is not None:
                self._store.write_gap_anchor(self._gap_anchor)
                self._store.compact(self._events)
            return removed

    def build_gap(self, requested_from_time: int) -> Gap:
        """构造缺口报告：请求时间早于最老可用事件时调用。"""
        with self._lock:
            oldest = self._events[0] if self._events else None
            anchor = self._gap_anchor
            if anchor is not None:
                gap_from = anchor.event_time
                lost_from, lost_to = anchor.seq, (oldest.seq - 1 if oldest else self._next_seq - 1)
            else:
                # 没有 trim 过却找不到数据：日志本来就空 / 请求时间在建日志之前
                gap_from = requested_from_time
                lost_from = lost_to = None
            gap_to = oldest.event_time if oldest is not None else requested_from_time
            return Gap(
                topic=self.topic,
                requested_from_time=requested_from_time,
                oldest_available=oldest,
                gap_from_time=gap_from,
                gap_to_time=gap_to,
                lost_from_seq=lost_from,
                lost_to_seq=lost_to,
            )
