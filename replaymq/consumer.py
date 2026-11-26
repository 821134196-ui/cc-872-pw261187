"""推送式消费者：live 与 replay 跑在各自独立的工作线程上。

- 实时工作线程：持续 poll 实时游标，永远不受重放配额影响
- 重放工作线程：每个 ReplaySession 一个线程，遇到 RateLimited 按建议退避（降速），
  会话 FINISHED/GAP/FAILED 后退出
- handler 收到的 Envelope 带 cursor_kind，业务侧可据此区分实时处理与补账处理
"""

from __future__ import annotations

import threading
from typing import Callable, Optional

from .broker import Broker, Envelope
from .cursor import RateLimited, ReplayState

Handler = Callable[[Envelope], bool]  # 返回 True=ack，False=nack 重投


class Consumer:
    def __init__(
        self,
        broker: Broker,
        name: str,
        topic: str,
        live_handler: Handler,
        replay_handler: Optional[Handler] = None,
        poll_batch: int = 10,
        idle_sleep_ms: int = 50,
    ):
        self.broker = broker
        self.name = name
        self.topic = topic
        self.live_handler = live_handler
        self.replay_handler = replay_handler or live_handler
        self.poll_batch = poll_batch
        self.idle_sleep_ms = idle_sleep_ms

        self.live_cursor = broker.subscribe(name, topic)
        self._stop = threading.Event()
        self._workers: list[threading.Thread] = []
        self._sessions: dict[str, object] = {}

    # ---------- 生命周期 ----------

    def start(self) -> None:
        self._stop.clear()
        t = threading.Thread(
            target=self._run_live, name=f"{self.name}-live", daemon=True
        )
        t.start()
        self._workers.append(t)

    def start_replay(self, from_time_ms: int, **replay_kwargs) -> object:
        session = self.broker.start_replay(
            self.name, self.topic, from_time_ms, **replay_kwargs
        )
        self._sessions[session.session_id] = session
        if session.state == ReplayState.GAP:
            return session  # 缺口留给业务显式确认，不起工作线程
        t = threading.Thread(
            target=self._run_replay,
            name=f"{self.name}-{session.session_id}",
            args=(session,),
            daemon=True,
        )
        t.start()
        self._workers.append(t)
        return session

    def stop(self, timeout: float = 5.0) -> None:
        self._stop.set()
        for t in self._workers:
            t.join(timeout=timeout)

    # ---------- 工作循环 ----------

    def _sleep(self, ms: int) -> None:
        self._stop.wait(timeout=max(ms, 1) / 1000.0)

    def _run_live(self) -> None:
        cursor_id = self.live_cursor.cursor_id
        while not self._stop.is_set():
            envelopes = self.broker.poll(cursor_id, self.poll_batch)
            if not envelopes:
                self._sleep(self.idle_sleep_ms)
                continue
            for env in envelopes:
                if self._stop.is_set():
                    return
                self._handle(env, self.live_handler)

    def _run_replay(self, session) -> None:
        cursor_id = session.cursor.cursor_id
        while not self._stop.is_set():
            if session.state in (ReplayState.FINISHED, ReplayState.FAILED, ReplayState.GAP):
                return
            try:
                envelopes = self.broker.poll(cursor_id, self.poll_batch)
            except RateLimited as exc:
                # 重放占用过高被降速：按令牌桶建议退避，实时线程完全不受影响
                self._sleep(max(exc.retry_after_ms, 10))
                continue
            if not envelopes:
                if session.state == ReplayState.FINISHED:
                    return
                self._sleep(self.idle_sleep_ms)
                continue
            for env in envelopes:
                if self._stop.is_set():
                    return
                self._handle(env, self.replay_handler)

    def _handle(self, env: Envelope, handler: Handler) -> None:
        try:
            ok = handler(env)
        except Exception:
            ok = False
        if ok:
            self.broker.ack(env.cursor_id, env.event.seq)
        else:
            self.broker.nack(env.cursor_id, env.event.seq)
