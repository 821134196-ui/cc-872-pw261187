"""推送式消费者集成测试：live / replay 独立工作线程并行工作。"""

import threading
import time
import unittest

from replaymq import Broker, Envelope
from replaymq.cursor import RateLimited, ReplayState
from tests.util import FakeClock


class ConsumerThreadingTests(unittest.TestCase):
    def test_live_and_replay_handlers_run_in_parallel(self):
        clock = FakeClock()
        broker = Broker(clock=clock, visibility_timeout_ms=5000)
        for i in range(4):
            clock.advance(1)
            broker.publish("orders", f"hist-{i}", {"i": i})

        received_live: list[str] = []
        received_replay: list[str] = []
        lock = threading.Lock()

        def live_handler(env: Envelope) -> bool:
            with lock:
                received_live.append(env.event.event_id)
            return True

        def replay_handler(env: Envelope) -> bool:
            with lock:
                received_replay.append(env.event.event_id)
            return True

        from replaymq import Consumer
        consumer = Consumer(
            broker, "acct-svc", "orders",
            live_handler=live_handler, replay_handler=replay_handler,
            idle_sleep_ms=5,
        )
        consumer.start()
        try:
            session = consumer.start_replay(from_time_ms=0)
            self.assertIn(session.state, (ReplayState.RUNNING, ReplayState.FINISHED))

            # 重放跑历史的同时，实时线程持续接收新事件
            for i in range(3):
                clock.advance(1)
                broker.publish("orders", f"live-{i}", {"i": i})

            deadline = time.time() + 3
            while time.time() < deadline:
                if len(received_replay) >= 4 and len(received_live) >= 3:
                    break
                time.sleep(0.02)

            self.assertEqual(sorted(received_replay),
                             sorted(f"hist-{i}" for i in range(4)))
            self.assertEqual(sorted(received_live),
                             sorted(f"live-{i}" for i in range(3)))
            self.assertEqual(session.state, ReplayState.FINISHED)

            # 两个游标各自独立提交
            cursors = {c["kind"]: c for c in broker.cursors("orders")}
            self.assertEqual(cursors["replay"]["committed_seq"], 3)
            self.assertEqual(cursors["live"]["committed_seq"], 6)
        finally:
            consumer.stop()

    def test_replay_backs_off_when_rate_limited_without_blocking_live(self):
        clock = FakeClock()
        broker = Broker(clock=clock)
        events = []
        for i in range(6):
            clock.advance(1)
            events.append(broker.publish("orders", f"e-{i}", {"i": i}))
        size = events[0].size_bytes

        live_seen = threading.Event()

        def live_handler(env: Envelope) -> bool:
            live_seen.set()
            return True

        replay_count = {"n": 0}
        count_lock = threading.Lock()

        def replay_handler(env: Envelope) -> bool:
            with count_lock:
                replay_count["n"] += 1
            return True

        from replaymq import Consumer
        # 总配额只够 2 条：重放线程投递 2 条后被限流，进入退避循环
        consumer = Consumer(
            broker, "acct-svc", "orders",
            live_handler=live_handler, replay_handler=replay_handler,
            idle_sleep_ms=2,
        )
        consumer.start()
        try:
            session = consumer.start_replay(
                from_time_ms=0,
                rate_bytes_per_sec=10_000, burst_bytes=100_000,
                quota_total_bytes=size * 2,
            )
            deadline = time.time() + 3
            while time.time() < deadline and replay_count["n"] < 2:
                time.sleep(0.02)
            self.assertEqual(replay_count["n"], 2)
            # 重放已因总配额进入 FAILED，工作线程退出
            deadline = time.time() + 2
            while time.time() < deadline and session.state != ReplayState.FAILED:
                time.sleep(0.02)
            self.assertEqual(session.state, ReplayState.FAILED)

            # 关键断言：重放被限流/失败期间，实时通道完全不受影响
            clock.advance(1)
            broker.publish("orders", "fresh", {})
            self.assertTrue(live_seen.wait(timeout=3),
                            "实时通道在重放被限流期间必须照常工作")
        finally:
            consumer.stop()

    def test_pause_and_dynamic_throttle_via_broker(self):
        """暂停期间重放线程持续退避；恢复后补账完成。"""
        clock = FakeClock()
        broker = Broker(clock=clock)
        for i in range(5):
            clock.advance(1)
            broker.publish("orders", f"e-{i}", {"i": i})

        done = threading.Event()
        count = {"n": 0}
        lk = threading.Lock()

        def replay_handler(env: Envelope) -> bool:
            with lk:
                count["n"] += 1
                if count["n"] == 5:
                    done.set()
            return True

        from replaymq import Consumer
        consumer = Consumer(
            broker, "acct-svc", "orders",
            live_handler=lambda env: True, replay_handler=replay_handler,
            idle_sleep_ms=2,
        )
        consumer.start()
        try:
            session = consumer.start_replay(from_time_ms=0)
            broker.pause_replay(session.session_id)
            time.sleep(0.15)
            paused_count = count["n"]
            # 暂停期间不会有新投递
            time.sleep(0.1)
            self.assertEqual(count["n"], paused_count)
            broker.resume_replay(session.session_id)
            self.assertTrue(done.wait(timeout=3))
            self.assertEqual(session.state, ReplayState.FINISHED)
        finally:
            consumer.stop()


if __name__ == "__main__":
    unittest.main()
