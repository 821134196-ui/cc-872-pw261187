"""启动只读运维页面，演示实时/补账对齐视图。

运行：python3 run_dashboard.py
然后浏览器打开 http://127.0.0.1:8077

启动后会自动制造：历史事件 -> 一次命中缺口的补账 -> 持续实时流量，
页面每 2 秒刷新，可观察 LIVE / REPLAY 两条记录的提交状态变化。
"""

import threading
import time

from replaymq import Broker
from replaymq.cursor import ReplayState
from replaymq.dashboard import serve

TOPIC = "orders"
CONSUMER = "acct-svc"


def seed_and_run(broker: Broker) -> None:
    # 1. 历史流量
    for i in range(6):
        broker.publish(TOPIC, f"hist-{i}", {"amount": i})
        time.sleep(0.05)

    live = broker.subscribe(CONSUMER, TOPIC)

    # 2. 从更早时间补账（本演示无 trim，故为全量重放，不产生缺口）
    session = broker.start_replay(
        CONSUMER, TOPIC, from_time_ms=0,
        rate_bytes_per_sec=2_000, burst_bytes=4_000,
    )
    replay_id = session.cursor.cursor_id

    def drain(cursor_id, ack=True):
        while True:
            try:
                batch = broker.poll(cursor_id, 5)
            except Exception:
                time.sleep(0.1)
                continue
            if not batch:
                return
            for env in batch:
                if ack:
                    broker.ack(cursor_id, env.event.seq)
                time.sleep(0.05)

    replay_worker = threading.Thread(
        target=drain, args=(replay_id,), name="demo-replay", daemon=True
    )
    replay_worker.start()

    # 3. 持续实时流量；重放边界之后的事件只进实时游标
    seq = 0
    while True:
        broker.publish(TOPIC, f"live-{seq}", {"ts": time.time()})
        drain(live.cursor_id)
        seq += 1
        time.sleep(0.5)


def main() -> None:
    broker = Broker(retention_max_events=50)
    worker = threading.Thread(target=seed_and_run, args=(broker,), daemon=True)
    worker.start()

    httpd = serve(broker, TOPIC, port=8077)
    print("Dashboard: http://127.0.0.1:8077  (Ctrl+C 退出)")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        httpd.shutdown()


if __name__ == "__main__":
    main()
