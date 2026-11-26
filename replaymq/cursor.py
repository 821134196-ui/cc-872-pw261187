"""消费游标与重放会话。

两类游标：
  live    实时游标，每个消费者一个，从"创建之后"的新事件开始投递
  replay  重放游标，从指定时间定位，属于某个 ReplaySession，追到实时位点后结束

游标各自独立维护 in-flight 与已提交位点，互不影响；
ReplaySession 额外持有独立的 token bucket（字节/秒）和总字节配额，
可以在运行中降速、暂停，且不占用实时通道的任何资源额度。
"""

from __future__ import annotations

import enum
import threading
from dataclasses import dataclass
from typing import Optional

from .storage import Event, Gap


class ReplayState(str, enum.Enum):
    RUNNING = "RUNNING"
    PAUSED = "PAUSED"
    FINISHED = "FINISHED"   # 已追过重放边界
    FAILED = "FAILED"       # 重放过程中出错（如配额耗尽被终止）
    GAP = "GAP"             # 起始时间早于最老可用事件，无法重放


class RateLimited(Exception):
    """重放流量超过独立配额 / 被暂停时抛出，实时通道永不抛此异常。"""

    def __init__(self, reason: str, retry_after_ms: int = 0):
        super().__init__(reason)
        self.reason = reason
        self.retry_after_ms = retry_after_ms


class TokenBucket:
    """字节令牌桶：rate_bytes_per_sec 持续补充，burst_bytes 为桶容量。

    时钟由外部注入（broker 的 monotonic clock），测试可完全确定化。
    """

    def __init__(self, rate_bytes_per_sec: float, burst_bytes: int, clock):
        self._rate = float(rate_bytes_per_sec)
        self._capacity = int(burst_bytes)
        self._tokens = float(burst_bytes)
        self._clock = clock
        self._updated = clock()
        self._lock = threading.Lock()

    @property
    def rate(self) -> float:
        return self._rate

    def _refill(self, now: float) -> None:
        elapsed = max(0.0, now - self._updated)
        self._tokens = min(self._capacity, self._tokens + elapsed * self._rate)
        self._updated = now

    def acquire(self, need_bytes: int) -> tuple[bool, int]:
        """尝试消费 need_bytes 令牌。

        返回 (是否允许, 建议等待毫秒)。允许时令牌已扣减。
        """
        with self._lock:
            now = self._clock()
            self._refill(now)
            if self._tokens >= need_bytes:
                self._tokens -= need_bytes
                return True, 0
            deficit = need_bytes - self._tokens
            wait_ms = int((deficit / self._rate) * 1000) + 1 if self._rate > 0 else 10**9
            return False, wait_ms

    def update_rate(self, rate_bytes_per_sec: float) -> None:
        with self._lock:
            now = self._clock()
            self._refill(now)
            self._rate = float(rate_bytes_per_sec)

    def drain(self) -> None:
        """排空存量令牌：紧急降速时立即生效，而不是等存量令牌慢慢耗尽。"""
        with self._lock:
            self._updated = self._clock()
            self._tokens = 0.0

    def refill_to_capacity(self) -> None:
        """提速恢复时立即补满，避免空桶卡住恢复后的首次投递。"""
        with self._lock:
            self._updated = self._clock()
            self._tokens = float(self._capacity)

    def available(self) -> float:
        with self._lock:
            self._refill(self._clock())
            return self._tokens


@dataclass
class InFlight:
    event: Event
    delivered_at_ms: int
    attempts: int


class Cursor:
    """一个消费位点。live 与 replay 同构，差异只在 kind 与归属会话。"""

    def __init__(self, cursor_id: str, kind: str, topic: str, start_seq: int):
        if kind not in ("live", "replay"):
            raise ValueError(f"unknown cursor kind: {kind}")
        self.cursor_id = cursor_id
        self.kind = kind
        self.topic = topic
        self.next_seq = start_seq          # 下一条待投递 seq
        self.committed_seq = start_seq - 1  # 已连续提交的最高 seq
        self.in_flight: dict[int, InFlight] = {}
        self._pending_acks: set[int] = set()  # 乱序 ack 暂存
        # 投递次数独立于 in_flight：nack/超时移出 in_flight 后仍保留，重投时 +1
        self._attempts: dict[int, int] = {}
        self.lock = threading.RLock()

    # ---------- 位点推进 ----------

    def claim(self, event: Event, now_ms: int) -> None:
        """标记事件为投递中。重投时 attempts 累加。"""
        with self.lock:
            attempts = self._attempts.get(event.seq, 0) + 1
            self._attempts[event.seq] = attempts
            self.in_flight[event.seq] = InFlight(event, now_ms, attempts)
            if event.seq >= self.next_seq:
                self.next_seq = event.seq + 1

    def ack(self, seq: int) -> tuple[bool, int]:
        """确认一条事件。

        返回 (committed 是否推进, 推进后的 committed_seq)。
        乱序 ack 会暂存，等空洞补齐后一次性连续推进。
        """
        with self.lock:
            if seq <= self.committed_seq:
                return False, self.committed_seq
            self.in_flight.pop(seq, None)
            self._attempts.pop(seq, None)
            self._pending_acks.add(seq)
            advanced = False
            while (self.committed_seq + 1) in self._pending_acks:
                self.committed_seq += 1
                self._pending_acks.discard(self.committed_seq)
                advanced = True
            return advanced, self.committed_seq

    def nack(self, seq: int) -> None:
        """否定确认：立即可重新投递（at-least-once）。"""
        with self.lock:
            self.in_flight.pop(seq, None)
            self._pending_acks.discard(seq)
            # 回退 next_seq，使 poll 能再次取到
            if seq < self.next_seq:
                self.next_seq = seq

    def reclaim_timeouts(self, now_ms: int, visibility_ms: int) -> list[Event]:
        """超过可见性超时仍未 ack 的 in-flight 事件重新可投。"""
        expired: list[Event] = []
        with self.lock:
            for seq, item in list(self.in_flight.items()):
                if now_ms - item.delivered_at_ms >= visibility_ms:
                    expired.append(item.event)
                    self.in_flight.pop(seq, None)
                    if seq < self.next_seq:
                        self.next_seq = seq
            return expired

    # ---------- 观测 ----------

    def pin_seq(self) -> int:
        """保留 pin：此游标仍需要的最老 seq（trim 不得越过它）。"""
        with self.lock:
            if self.in_flight:
                return min(self.in_flight)
            return self.committed_seq + 1

    def is_caught_up(self, log_hi_seq: int) -> bool:
        with self.lock:
            return not self.in_flight and self.next_seq > log_hi_seq

    def snapshot(self) -> dict:
        with self.lock:
            return {
                "cursor_id": self.cursor_id,
                "kind": self.kind,
                "committed_seq": self.committed_seq,
                "next_seq": self.next_seq,
                "in_flight": sorted(self.in_flight.keys()),
                "pin_seq": self.pin_seq(),
            }


class ReplaySession:
    """一次历史重放的完整运行时状态。"""

    def __init__(
        self,
        session_id: str,
        consumer: str,
        cursor: Cursor,
        from_time: int,
        rate_bytes_per_sec: float,
        burst_bytes: int,
        quota_total_bytes: int,
        clock,
        gap: Optional[Gap] = None,
    ):
        self.session_id = session_id
        self.consumer = consumer
        self.cursor = cursor
        self.from_time = from_time
        self.bucket = TokenBucket(rate_bytes_per_sec, burst_bytes, clock)
        self.quota_total_bytes = quota_total_bytes
        self.quota_used_bytes = 0
        self.state: ReplayState = ReplayState.GAP if gap is not None else ReplayState.RUNNING
        self.gap: Optional[Gap] = gap
        self.created_at_ms = clock_ms(clock)
        self.finished_at_ms: Optional[int] = None
        self.delivered = 0
        self.lock = threading.RLock()

    def charge(self, size_bytes: int) -> None:
        """重放投递前的配额闸门。实时通道不走这里。"""
        with self.lock:
            if self.state == ReplayState.PAUSED:
                raise RateLimited("重放会话已被手动暂停", retry_after_ms=1000)
            if self.state in (ReplayState.FINISHED, ReplayState.FAILED, ReplayState.GAP):
                raise RateLimited(f"重放会话状态为 {self.state.value}，不再接受投递")
            if self.quota_used_bytes + size_bytes > self.quota_total_bytes:
                self.state = ReplayState.FAILED
                raise RateLimited(
                    f"重放总配额耗尽：{self.quota_used_bytes}+{size_bytes} > {self.quota_total_bytes}",
                )
            ok, wait_ms = self.bucket.acquire(size_bytes)
            if not ok:
                raise RateLimited("重放速率超过独立配额，触发降速", retry_after_ms=wait_ms)
            self.quota_used_bytes += size_bytes
            self.delivered += 1

    def set_rate(self, rate_bytes_per_sec: float, drain: bool = False) -> None:
        """动态降速 / 提速（不影响实时通道）。

        drain=True 时同时排空存量令牌，用于占用过高的紧急降速，立即生效。
        提速时把令牌补到新桶容量，避免恢复后仍被空桶卡住。
        """
        with self.lock:
            old_rate = self.bucket.rate
            self.bucket.update_rate(rate_bytes_per_sec)
            if drain:
                self.bucket.drain()
            elif rate_bytes_per_sec > old_rate:
                self.bucket.refill_to_capacity()

    def pause(self) -> None:
        with self.lock:
            if self.state == ReplayState.RUNNING:
                self.state = ReplayState.PAUSED

    def resume(self) -> None:
        with self.lock:
            if self.state == ReplayState.PAUSED:
                self.state = ReplayState.RUNNING

    def finish(self, clock) -> None:
        with self.lock:
            if self.state in (ReplayState.RUNNING, ReplayState.PAUSED):
                self.state = ReplayState.FINISHED
                self.finished_at_ms = clock_ms(clock)

    def snapshot(self) -> dict:
        with self.lock:
            return {
                "session_id": self.session_id,
                "consumer": self.consumer,
                "state": self.state.value,
                "from_time": self.from_time,
                "rate_bytes_per_sec": self.bucket.rate,
                "quota_used_bytes": self.quota_used_bytes,
                "quota_total_bytes": self.quota_total_bytes,
                "delivered": self.delivered,
                "gap": self.gap.describe() if self.gap else None,
                "cursor": self.cursor.snapshot(),
            }


def clock_ms(clock) -> int:
    """把注入的秒级 monotonic clock 转成毫秒。"""
    return int(clock() * 1000)
