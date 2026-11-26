"""端到端演示：历史重放的完整语义。

运行：python3 demo.py
场景顺序：
  1. 历史事件 + 保留策略制造已清除区间
  2. 消费者从指定时间补账 -> 命中缺口，明确报告缺口区间
  3. 确认缺口后从最老可用事件重放；同时新流量走实时游标正常投递
  4. 同一事件被实时 / 补账两个游标分别投递，对齐视图展示两条记录
  5. 两个游标并存时 ack 的归属凭证（哪个结果已提交）
  6. 重放占用过高 -> 动态降速 / 暂停，且不影响实时通道
  7. 未完成重放的 pin 保住最老可用事件，完成后保留策略才继续清理
"""

from replaymq import Broker
from replaymq.cursor import RateLimited, ReplayState
from tests.util import FakeClock


def hr(title: str) -> None:
    print("\n" + "=" * 72)
    print(f"  {title}")
    print("=" * 72)


def main() -> None:
    clock = FakeClock()
    broker = Broker(retention_max_events=4, clock=clock)
    topic = "orders"
    consumer = "acct-svc"

    # ---------- 1. 制造历史与保留窗口 ----------
    hr("1. 发布 8 条历史事件；保留窗口=4，seq 0..3 已被清除")
    for i in range(8):
        clock.advance(10)
        broker.publish(topic, f"order-{i}", {"amount": 100 + i})
    stats = broker.topic_stats(topic)
    print(f"日志现状: lo_seq={stats['lo_seq']} hi_seq={stats['hi_seq']} "
          f"gap_anchor={stats['gap_anchor']}")

    # ---------- 2. 从指定时间补账，命中缺口 ----------
    hr("2. 消费者从 time=0 补账 -> 数据不全，明确指出缺口")
    live = broker.subscribe(consumer, topic)
    session = broker.start_replay(consumer, topic, from_time_ms=0)
    print(f"重放会话: {session.session_id} 状态={session.state.value}")
    print(session.gap.describe())

    # ---------- 3. 确认缺口后重放；新流量并行 ----------
    hr("3. 确认缺口，从最老可用事件补账；同时新流量正常投递实时通道")
    broker.acknowledge_gap(session.session_id)
    replay_id = session.cursor.cursor_id
    replayed = broker.poll(replay_id, max_events=10)
    print(f"补账投递 {len(replayed)} 条: "
                  f"{[e.event.event_id for e in replayed]}")
    for env in replayed:
        broker.ack(replay_id, env.event.seq)
    print(f"补账会话状态: {session.state.value}（边界内全部提交后结束）")

    clock.advance(10)
    fresh = broker.publish(topic, "order-live-1", {"amount": 999})
    live_batch = broker.poll(live.cursor_id, 10)
    print(f"实时通道新流量: {[e.event.event_id for e in live_batch]} "
          f"(cursor={live.cursor_id})，与已结束的重放互不干扰")

    # ---------- 4. 同一事件跨边界：两条记录对齐 ----------
    hr("4. 新一轮重放覆盖实时事件 -> 同一事件的 LIVE/REPLAY 两条记录对齐")
    session2 = broker.start_replay(consumer, topic, from_time_ms=0)
    rid2 = session2.cursor.cursor_id
    # session2 同样命中缺口（seq 0..3 已丢），确认后继续
    if session2.state == ReplayState.GAP:
        print(session2.gap.describe())
        broker.acknowledge_gap(session2.session_id)
    broker.poll(rid2, 10)

    view = {row["event_id"]: row for row in broker.alignment_view(topic)}
    row = view["order-live-1"]
    print(f"事件 order-live-1 (seq={row['seq']}):")
    print(f"  LIVE   记录: {row['live']}")
    print(f"  REPLAY 记录: {row['replay']}")
    print("  -> 同一 event_id，两条来源独立记录、独立提交状态")

    # ---------- 5. ack 归属 ----------
    hr("5. 两个游标并存：ack 必须区分哪个结果已提交")
    r_live = broker.ack(live.cursor_id, fresh.seq)
    print("提交实时结果: " + r_live.describe())
    view = {r["event_id"]: r for r in broker.alignment_view(topic)}
    row = view["order-live-1"]
    print(f"对齐视图刷新: LIVE committed={row['live'][0]['committed']}, "
          f"REPLAY committed={row['replay'][0]['committed']}（补账那份仍未提交）")
    # 先补齐补账游标的前序事件，再提交目标事件 —— 演示乱序 ack 的连续提交语义
    for seq in range(session2.cursor.committed_seq + 1, fresh.seq):
        broker.ack(rid2, seq)
    r_replay = broker.ack(rid2, fresh.seq)
    print("提交补账结果: " + r_replay.describe())
    print(f"补账会话最终状态: {session2.state.value}（边界内全部提交）")

    # ---------- 6. 降速 / 暂停 ----------
    hr("6. 重放占用过高：动态降速与暂停（独立配额，实时通道不受影响）")
    for i in range(6):
        clock.advance(10)
        broker.publish(topic, f"burst-{i}", {})
    session3 = broker.start_replay(
        consumer, topic, from_time_ms=0,
        rate_bytes_per_sec=10_000, burst_bytes=100_000,
    )
    if session3.state == ReplayState.GAP:
        broker.acknowledge_gap(session3.session_id)
    rid3 = session3.cursor.cursor_id
    first_two = broker.poll(rid3, 2)
    for env in first_two:
        broker.ack(rid3, env.event.seq)
    broker.throttle_replay(session3.session_id, rate_bytes_per_sec=1, drain=True)
    print(f"已紧急降速到 {session3.bucket.rate} 字节/秒并排空存量令牌；尝试继续拉取:")
    try:
        broker.poll(rid3, 10)
    except RateLimited as exc:
        print(f"  -> 被限流: {exc.reason}，建议退避 {exc.retry_after_ms}ms")
    broker.pause_replay(session3.session_id)
    print("会话已暂停；实时通道验证:")
    clock.advance(10)
    broker.publish(topic, "order-live-2", {})
    live2 = broker.poll(live.cursor_id, 10)
    print(f"  实时投递正常: {[e.event.event_id for e in live2]}")
    broker.resume_replay(session3.session_id)
    broker.throttle_replay(session3.session_id, rate_bytes_per_sec=10_000_000)
    rest = broker.poll(rid3, 20)
    for env in rest:
        broker.ack(rid3, env.event.seq)
    print(f"恢复并提速后补账完成，会话状态={session3.state.value}")

    # ---------- 7. pin 保住最老可用事件 ----------
    hr("7. 保留策略与 pin：未确认的重放钉住最老可用事件")
    session4 = broker.start_replay(consumer, topic, from_time_ms=0)
    if session4.state == ReplayState.GAP:
        broker.acknowledge_gap(session4.session_id)
    rid4 = session4.cursor.cursor_id
    holding = broker.poll(rid4, 1)  # 不 ack
    pin = holding[0].event.seq
    for i in range(8):
        clock.advance(10)
        broker.publish(topic, f"tail-{i}", {})
    stats = broker.topic_stats(topic)
    print(f"重放停在 seq={pin} 未确认，大量新事件写入后 lo_seq={stats['lo_seq']}（== pin，未被清除）")
    assert stats["lo_seq"] == pin
    # 先确认钉住的第一条，再把其余在途/待投递事件全部确认，重放 pin 才完全解除
    broker.ack(rid4, pin)
    for env in broker.poll(rid4, 50):
        broker.ack(rid4, env.event.seq)
    # 实时游标同样要确认全部在途事件（含场景 6 投递的 burst 系列），
    # 它的保留 pin 才会释放 —— 两类游标都是保留策略的约束方
    clock.advance(broker.visibility_timeout_ms + 1)  # 兜底：超时未 ack 的会重投
    while True:
        pending = broker.poll(live.cursor_id, 50)
        if not pending:
            break
        for env in pending:
            broker.ack(live.cursor_id, env.event.seq)
    clock.advance(10)
    broker.publish(topic, "tail-final", {})
    stats = broker.topic_stats(topic)
    print(f"补账全部确认、所有 pin 解除后，再写入触发 trim: lo_seq={stats['lo_seq']}（已前移）")
    assert stats["lo_seq"] > pin

    hr("全部场景完成")
    print("重放会话快照:")
    for s in broker.replay_sessions():
        print(f"  {s['session_id']}: state={s['state']} delivered={s['delivered']} "
              f"quota={s['quota_used_bytes']}/{s['quota_total_bytes']}B "
              f"gap={'有' if s['gap'] else '无'}")


if __name__ == "__main__":
    main()
