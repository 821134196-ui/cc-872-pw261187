"""历史重放核心语义测试。

覆盖需求：
1. 重放期间新流量正常投递，实时/重放使用独立游标
2. 起始时间早于最老可用事件 -> 结构化缺口 Gap，明确指出缺口区间
3. 同一事件跨过重放边界 -> 实时与补账两条记录按 event_id 对齐
4. 同一消费者两个游标 -> ack 必须区分哪个结果已提交
5. 重放独立配额：速率令牌桶降速、总配额耗尽失败，均不影响实时通道
6. 保留策略 trim 不得越过未完成重放的 pin（保住最老可用事件）
7. at-least-once：超时重投 / nack 重投 / 乱序 ack 连续提交
"""

import os
import shutil
import tempfile
import unittest

from replaymq import Broker
from replaymq.cursor import RateLimited, ReplayState
from tests.util import FakeClock


def make_broker(**kwargs):
    clock = FakeClock()
    return Broker(clock=clock, **kwargs), clock


class ReplayFlowTests(unittest.TestCase):
    def setUp(self):
        self.broker, self.clock = make_broker()

    def publish_n(self, n, prefix="evt"):
        # 每条事件推进 1ms，保证 event_time 严格递增（基于时间的重放定位才有区分度）
        result = []
        for i in range(n):
            self.clock.advance(1)
            result.append(self.broker.publish("orders", f"{prefix}-{i}", {"i": i}))
        return result

    # ---------- 1. 独立游标：重放与新流量并行 ----------

    def test_replay_runs_while_live_traffic_continues(self):
        self.publish_n(5)
        live = self.broker.subscribe("acct-svc", "orders")
        session = self.broker.start_replay("acct-svc", "orders", from_time_ms=0)

        self.assertEqual(session.state, ReplayState.RUNNING)
        replay_id = session.cursor.cursor_id
        live_id = live.cursor_id
        self.assertNotEqual(replay_id, live_id)

        # 重放投递历史 5 条
        replayed = self.broker.poll(replay_id, max_events=10)
        self.assertEqual([e.event.seq for e in replayed], [0, 1, 2, 3, 4])
        self.assertTrue(all(e.cursor_kind == "replay" for e in replayed))

        # 重放进行中，新流量照常进入实时通道
        new_events = self.publish_n(3, prefix="new")
        live_batch = self.broker.poll(live_id, max_events=10)
        self.assertEqual([e.event.event_id for e in live_batch], ["new-0", "new-1", "new-2"])
        self.assertTrue(all(e.cursor_kind == "live" for e in live_batch))
        # 新事件不属于重放（边界固定为发起时的 hi_seq=4）；未 ack 前会话仍在运行
        self.assertEqual(session.state, ReplayState.RUNNING)
        again = self.broker.poll(replay_id, max_events=10)
        self.assertEqual(again, [])
        # 边界内全部确认后，补账才算完成
        for env in replayed:
            self.broker.ack(replay_id, env.event.seq)
        self.assertEqual(session.state, ReplayState.FINISHED)

    # ---------- 2. 缺口：查不到最早数据时明确指出 ----------

    def test_gap_reported_when_history_trimmed(self):
        self.publish_n(8)
        # 没有任何游标，直接 trim 掉前 5 条，模拟保留策略已清除的历史
        self.broker._topics["orders"].trim_before(5)
        stats = self.broker.topic_stats("orders")
        self.assertEqual(stats["lo_seq"], 5)
        self.assertEqual(stats["gap_anchor"]["seq"], 0)

        session = self.broker.start_replay("acct-svc", "orders", from_time_ms=0)
        self.assertEqual(session.state, ReplayState.GAP)
        gap = session.gap
        self.assertIsNotNone(gap)
        self.assertEqual((gap.lost_from_seq, gap.lost_to_seq), (0, 4))
        self.assertEqual(gap.oldest_available.seq, 5)
        message = gap.describe()
        self.assertIn("缺口", message)
        self.assertIn("seq[0..4]", message)
        self.assertIn("最老可用事件", message)

        # 确认缺口后从最老可用事件继续
        self.broker.acknowledge_gap(session.session_id)
        batch = self.broker.poll(session.cursor.cursor_id, 10)
        self.assertEqual([e.event.seq for e in batch], [5, 6, 7])

    def test_empty_log_is_empty_replay_not_gap(self):
        broker, _ = make_broker()
        # 从未有过数据 -> 空重放（没有"已丢失"事件就谈不上缺口）
        session = broker.start_replay("ghost", "orders", from_time_ms=12345)
        self.assertEqual(session.state, ReplayState.FINISHED)
        self.assertIsNone(session.gap)

    def test_gap_when_trim_emptied_log(self):
        broker, clock = make_broker()
        for i in range(3):
            clock.advance(1)
            broker.publish("orders", f"e-{i}", {})
        broker._topics["orders"].trim_before(3)  # 全清，但 gap_anchor 保留
        session = broker.start_replay("acct-svc", "orders", from_time_ms=0)
        self.assertEqual(session.state, ReplayState.GAP)
        self.assertIsNone(session.gap.oldest_available)
        self.assertIn("日志已空", session.gap.describe())
        self.assertEqual(session.gap.lost_from_seq, 0)
        self.assertEqual(session.gap.lost_to_seq, 2)

    def test_future_start_time_is_empty_replay_not_gap(self):
        self.publish_n(2)
        session = self.broker.start_replay("acct-svc", "orders", from_time_ms=10**9)
        self.assertEqual(session.state, ReplayState.FINISHED)
        self.assertIsNone(session.gap)

    # ---------- 3. 同一事件跨边界：两条记录对齐 ----------

    def test_same_event_aligned_across_replay_boundary(self):
        events = self.publish_n(4)
        # 先订阅实时游标并处理掉 seq 0..2，然后发起从时间 0 的重放
        live = self.broker.subscribe("acct-svc", "orders")
        first_live = self.broker.poll(live.cursor_id, 10)
        self.assertEqual(len(first_live), 0)  # 订阅在发布之后，不收旧事件

        session = self.broker.start_replay("acct-svc", "orders", from_time_ms=0)
        replay_id = session.cursor.cursor_id
        replay_batch = self.broker.poll(replay_id, 10)
        # 重放覆盖全部 4 条历史
        self.assertEqual([e.event.event_id for e in replay_batch],
                         [e.event_id for e in events])

        # 重放期间新事件 seq=4，同时被实时游标投递（跨边界场景）
        boundary_event = self.broker.publish("orders", "evt-boundary", {"x": 1})
        live_batch = self.broker.poll(live.cursor_id, 10)
        self.assertEqual([e.event.seq for e in live_batch], [boundary_event.seq])

        # 对齐视图：历史事件只有 replay 记录；新事件只有 live 记录
        view = {row["event_id"]: row for row in self.broker.alignment_view("orders")}
        self.assertEqual(len(view["evt-0"]["replay"]), 1)
        self.assertEqual(view["evt-0"]["live"], [])
        self.assertEqual(len(view["evt-boundary"]["live"]), 1)
        self.assertEqual(view["evt-boundary"]["replay"], [])

        # 让重放边界也覆盖同一事件：新会话从时间 0 重放，此时 boundary 已是历史
        session2 = self.broker.start_replay("acct-svc", "orders", from_time_ms=0)
        self.broker.poll(session2.cursor.cursor_id, 10)
        view = {row["event_id"]: row for row in self.broker.alignment_view("orders")}
        row = view["evt-boundary"]
        self.assertEqual(len(row["live"]), 1)
        self.assertEqual(len(row["replay"]), 1)
        # 两条记录初始都未提交
        self.assertFalse(row["live"][0]["committed"])
        self.assertFalse(row["replay"][0]["committed"])

        # 只提交实时游标那一份
        r = self.broker.ack(live.cursor_id, boundary_event.seq)
        self.assertEqual(r.cursor_kind, "live")
        self.assertTrue(r.committed_advanced)
        view = {row["event_id"]: row for row in self.broker.alignment_view("orders")}
        row = view["evt-boundary"]
        self.assertTrue(row["live"][0]["committed"])
        self.assertFalse(row["replay"][0]["committed"])  # 补账那份仍是未提交

        # 再提交补账游标那份
        r2 = self.broker.ack(session2.cursor.cursor_id, boundary_event.seq)
        self.assertEqual(r2.cursor_kind, "replay")
        view = {row["event_id"]: row for row in self.broker.alignment_view("orders")}
        self.assertTrue(view["evt-boundary"]["replay"][0]["committed"])

    # ---------- 4. 两个游标并存：ack 归属明确 ----------

    def test_ack_ownership_between_two_cursors(self):
        self.publish_n(3)
        live = self.broker.subscribe("acct-svc", "orders")
        session = self.broker.start_replay("acct-svc", "orders", from_time_ms=0)
        replay_id = session.cursor.cursor_id

        replayed = self.broker.poll(replay_id, 10)
        self.broker.publish("orders", "live-1", {})
        live_batch = self.broker.poll(live.cursor_id, 10)
        seq = replayed[0].event.seq

        # 同一个 seq 在两个游标各自存在 in-flight，互不干扰
        r_replay = self.broker.ack(replay_id, seq)
        r_live = self.broker.ack(live.cursor_id, live_batch[0].event.seq)
        self.assertEqual(r_replay.cursor_kind, "replay")
        self.assertEqual(r_replay.session_id, session.session_id)
        self.assertEqual(r_live.cursor_kind, "live")
        self.assertIsNone(r_live.session_id)

        cursors = {c["cursor_id"]: c for c in self.broker.cursors("orders")}
        self.assertEqual(cursors[replay_id]["committed_seq"], seq)
        # 实时游标只提交了它自己那一条（live 从 seq=3 开始，提交后 committed=3）
        self.assertEqual(cursors[live.cursor_id]["committed_seq"], live_batch[0].event.seq)

        with self.assertRaises(RuntimeError):
            self.broker.ack("live:unknown:orders", 0)

    def test_out_of_order_ack_commits_contiguously(self):
        self.publish_n(5)
        session = self.broker.start_replay("acct-svc", "orders", from_time_ms=0)
        cid = session.cursor.cursor_id
        self.broker.poll(cid, 10)
        r2 = self.broker.ack(cid, 2)  # 乱序：先 ack 2
        self.assertFalse(r2.committed_advanced)
        self.assertEqual(r2.committed_seq, -1)
        r0 = self.broker.ack(cid, 0)
        self.assertTrue(r0.committed_advanced)
        self.assertEqual(r0.committed_seq, 0)  # 1 缺着，只推进到 0
        r1 = self.broker.ack(cid, 1)
        self.assertEqual(r1.committed_seq, 2)  # 空洞补齐，连推到 2

    # ---------- 5. 独立配额：降速 / 暂停 / 总配额，实时不受影响 ----------

    def test_rate_limit_throttles_replay_only(self):
        # 每条事件 payload 序列化约 7~8 字节；桶容量只够 1 条
        events = self.publish_n(6)
        first_size = events[0].size_bytes
        session = self.broker.start_replay(
            "acct-svc", "orders", from_time_ms=0,
            rate_bytes_per_sec=10, burst_bytes=first_size, quota_total_bytes=10_000,
        )
        cid = session.cursor.cursor_id
        first = self.broker.poll(cid, 10)
        self.assertEqual(len(first), 1)  # 桶里初始令牌只放 1 条
        with self.assertRaises(RateLimited) as ctx:
            self.broker.poll(cid, 10)
        self.assertGreater(ctx.exception.retry_after_ms, 0)

        # 实时通道同一时刻完全不受影响
        live = self.broker.subscribe("acct-svc", "orders")
        self.broker.publish("orders", "fresh", {})
        live_batch = self.broker.poll(live.cursor_id, 10)
        self.assertEqual([e.event.event_id for e in live_batch], ["fresh"])

        # 时间推进后令牌补充，重放继续
        self.clock.advance(5_000)
        second = self.broker.poll(cid, 10)
        self.assertGreater(len(second), 0)

    def test_dynamic_throttle_and_pause(self):
        events = self.publish_n(20)
        size = events[0].size_bytes
        session = self.broker.start_replay(
            "acct-svc", "orders", from_time_ms=0,
            rate_bytes_per_sec=10_000, burst_bytes=100_000,
        )
        cid = session.cursor.cursor_id
        self.assertEqual(len(self.broker.poll(cid, 20)), 20)
        for env in self.broker._topics["orders"].iter_all():
            self.broker.ack(cid, env.seq)  # 全部确认 -> 补账完成
        self.assertEqual(session.state, ReplayState.FINISHED)

        # 新会话演示运行中降速 / 暂停
        session2 = self.broker.start_replay(
            "acct-svc", "orders", from_time_ms=0,
            rate_bytes_per_sec=10_000, burst_bytes=size,
        )
        cid2 = session2.cursor.cursor_id
        self.assertEqual(len(self.broker.poll(cid2, 20)), 1)  # 初始令牌只够 1 条
        # 占用过高 -> 降速到 1 字节/秒，时间推进也补不满一条
        self.broker.throttle_replay(session2.session_id, rate_bytes_per_sec=1)
        self.clock.advance(1)
        with self.assertRaises(RateLimited):
            self.broker.poll(cid2, 10)
        # 暂停：即使时间走过也不放行
        self.broker.pause_replay(session2.session_id)
        self.clock.advance(60_000)
        with self.assertRaises(RateLimited):
            self.broker.poll(cid2, 10)
        # 恢复并提速后恢复投递
        self.broker.resume_replay(session2.session_id)
        self.broker.throttle_replay(session2.session_id, rate_bytes_per_sec=10_000_000)
        batch = self.broker.poll(cid2, 20)
        self.assertGreater(len(batch), 0)

    def test_total_quota_exhaustion_fails_session(self):
        events = self.publish_n(10)
        size = events[0].size_bytes  # 约 8 字节/条
        session = self.broker.start_replay(
            "acct-svc", "orders", from_time_ms=0,
            rate_bytes_per_sec=10_000, burst_bytes=100_000,
            quota_total_bytes=size * 2,  # 只够 2 条
        )
        cid = session.cursor.cursor_id
        first = self.broker.poll(cid, 10)
        self.assertEqual(len(first), 2)  # 配额内的 2 条照常投递
        with self.assertRaises(RateLimited):
            self.broker.poll(cid, 10)    # 第 3 条撑爆总配额
        self.assertEqual(session.state, ReplayState.FAILED)
        snap = session.snapshot()
        self.assertGreater(snap["quota_used_bytes"], 0)

    # ---------- 6. 保留 pin：回放未完成时保住最老事件 ----------

    def test_retention_cannot_trim_pinned_oldest_event(self):
        broker, clock = make_broker(retention_max_events=3)
        for i in range(6):
            clock.advance(1)
            broker.publish("orders", f"e-{i}", {"i": i})
        # 无游标时保留策略生效：只剩最后 3 条，seq 0..2 已被清除
        self.assertEqual(broker.topic_stats("orders")["lo_seq"], 3)

        # 从时间 0 补账 -> 命中缺口；确认缺口后从最老可用 seq=3 继续
        session = broker.start_replay("acct-svc", "orders", from_time_ms=0)
        self.assertEqual(session.state, ReplayState.GAP)
        self.assertEqual(session.gap.lost_from_seq, 0)
        self.assertEqual(session.gap.lost_to_seq, 2)
        broker.acknowledge_gap(session.session_id)
        cid = session.cursor.cursor_id
        first = broker.poll(cid, 1)  # 投一条但暂不 ack，pin 钉在 3
        self.assertEqual(first[0].event.seq, 3)
        for i in range(6, 12):
            clock.advance(1)
            broker.publish("orders", f"e-{i}", {"i": i})
        stats = broker.topic_stats("orders")
        self.assertEqual(stats["lo_seq"], 3, "未完成重放的 pin 必须保住最老可用事件")

        # 把重放全部处理完（含第一条 in-flight），pin 解除后保留策略才能继续清
        for env in first:
            broker.ack(cid, env.event.seq)
        remaining = broker.poll(cid, 20)
        for env in remaining:
            broker.ack(cid, env.event.seq)
        self.assertEqual(session.state, ReplayState.FINISHED)
        # 再发布触发 trim：窗口只留最后 3 条，lo_seq 必须前移
        for _ in range(5):
            clock.advance(1)
            broker.publish("orders", "e-tail", {})
        stats = broker.topic_stats("orders")
        self.assertGreater(stats["lo_seq"], 3)

    def test_age_based_retention_respects_pins(self):
        broker, clock = make_broker(retention_max_age_ms=1000)
        for i in range(4):
            broker.publish("orders", f"old-{i}", {})
        clock.advance(500)
        for i in range(2):
            broker.publish("orders", f"new-{i}", {})
        session = broker.start_replay("acct-svc", "orders", from_time_ms=0)
        cid = session.cursor.cursor_id
        broker.poll(cid, 2)  # pin 钉在 seq=0
        clock.advance(2000)  # 全部事件按年龄都过期
        broker.publish("orders", "fresh", {})  # 触发 age trim
        self.assertEqual(broker.topic_stats("orders")["lo_seq"], 0)

    # ---------- 7. at-least-once ----------

    def test_nack_redelivery(self):
        self.publish_n(2)
        session = self.broker.start_replay("acct-svc", "orders", from_time_ms=0)
        cid = session.cursor.cursor_id
        batch = self.broker.poll(cid, 10)
        self.broker.nack(cid, batch[0].event.seq)
        again = self.broker.poll(cid, 10)
        self.assertEqual(again[0].event.seq, batch[0].event.seq)
        self.assertEqual(again[0].attempts, 2)

    def test_visibility_timeout_redelivery(self):
        broker, clock = make_broker(visibility_timeout_ms=1000)
        broker.publish("orders", "e", {})
        session = broker.start_replay("acct-svc", "orders", from_time_ms=0)
        cid = session.cursor.cursor_id
        broker.poll(cid, 10)
        clock.advance(999)
        self.assertEqual(broker.poll(cid, 10), [])  # 未超时
        clock.advance(2)
        again = broker.poll(cid, 10)
        self.assertEqual(len(again), 1)
        self.assertEqual(again[0].attempts, 2)

    # ---------- 8. 持久化 ----------

    def test_jsonl_persistence_survives_restart(self):
        tmp = tempfile.mkdtemp()
        try:
            clock = FakeClock()
            broker = Broker(store_dir=tmp, clock=clock)
            broker.publish("orders", "p-1", {"v": 1})
            broker.publish("orders", "p-2", {"v": 2})
            live = broker.subscribe("acct-svc", "orders")
            broker.publish("orders", "p-3", {"v": 3})
            batch = broker.poll(live.cursor_id, 10)
            for env in batch:
                broker.ack(live.cursor_id, env.event.seq)

            broker2 = Broker(store_dir=tmp, clock=clock)
            stats = broker2.topic_stats("orders")
            self.assertEqual(stats["events"], 3)
            self.assertEqual(stats["lo_seq"], 0)
            # 重启后重放历史仍然完整
            session = broker2.start_replay("acct-svc", "orders", from_time_ms=0)
            replayed = broker2.poll(session.cursor.cursor_id, 10)
            self.assertEqual([e.event.event_id for e in replayed], ["p-1", "p-2", "p-3"])
            # 重启后账本为空也不应报错（惰性补建）
            view = broker2.alignment_view("orders")
            self.assertEqual([row["event_id"] for row in view], ["p-1", "p-2", "p-3"])
            self.assertTrue(all(row["live"] == [] for row in view))
        finally:
            shutil.rmtree(tmp, ignore_errors=True)

    def test_gap_anchor_persists_across_restart(self):
        tmp = tempfile.mkdtemp()
        try:
            clock = FakeClock()
            broker = Broker(store_dir=tmp, retention_max_events=3, clock=clock)
            for i in range(6):
                clock.advance(10)
                broker.publish("orders", f"e-{i}", {})
            # 保留策略已 trim 掉 seq 0..2，gap_anchor 被持久化
            self.assertEqual(broker.topic_stats("orders")["lo_seq"], 3)

            broker2 = Broker(store_dir=tmp, clock=clock)
            stats = broker2.topic_stats("orders")
            self.assertEqual(stats["lo_seq"], 3)
            self.assertEqual(stats["gap_anchor"]["seq"], 0)
            # 重启后从时间 0 补账仍能正确报告缺口，而不是误判为正常重放
            session = broker2.start_replay("acct-svc", "orders", from_time_ms=0)
            self.assertEqual(session.state, ReplayState.GAP)
            self.assertEqual((session.gap.lost_from_seq, session.gap.lost_to_seq), (0, 2))
            self.assertTrue(os.path.exists(os.path.join(tmp, "orders.jsonl.meta")))
        finally:
            shutil.rmtree(tmp, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()
