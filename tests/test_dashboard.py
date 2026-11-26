"""Dashboard HTTP 接口冒烟测试。"""

import json
import threading
import unittest
import urllib.request

from replaymq import Broker
from replaymq.dashboard import serve
from tests.util import FakeClock


class DashboardTests(unittest.TestCase):
    def setUp(self):
        clock = FakeClock()
        self.broker = Broker(clock=clock)
        for i in range(3):
            clock.advance(1)
            self.broker.publish("orders", f"e-{i}", {"i": i})
        live = self.broker.subscribe("acct-svc", "orders")
        session = self.broker.start_replay("acct-svc", "orders", from_time_ms=0)
        replayed = self.broker.poll(session.cursor.cursor_id, 10)
        self.broker.ack(session.cursor.cursor_id, replayed[0].event.seq)
        clock.advance(1)
        self.broker.publish("orders", "e-live", {})
        self.broker.poll(live.cursor_id, 10)

        self.httpd = serve(self.broker, "orders", port=0)
        self.port = self.httpd.server_address[1]
        self.thread = threading.Thread(target=self.httpd.serve_forever, daemon=True)
        self.thread.start()

    def tearDown(self):
        self.httpd.shutdown()
        self.httpd.server_close()
        self.thread.join(timeout=2)

    def _get(self, path):
        with urllib.request.urlopen(f"http://127.0.0.1:{self.port}{path}", timeout=3) as resp:
            return resp.status, resp.read().decode()

    def test_health_and_stats(self):
        status, body = self._get("/api/health")
        self.assertEqual(status, 200)
        self.assertEqual(json.loads(body), {"ok": True})

        status, body = self._get("/api/stats")
        payload = json.loads(body)
        self.assertEqual(payload["topic"]["events"], 4)
        self.assertEqual(len(payload["sessions"]), 1)
        kinds = {c["kind"] for c in payload["cursors"]}
        self.assertEqual(kinds, {"live", "replay"})

    def test_alignment_shows_two_cursor_records(self):
        # 让重放也覆盖实时事件，制造同一事件的两条记录
        session2 = self.broker.start_replay("acct-svc", "orders", from_time_ms=0)
        self.broker.poll(session2.cursor.cursor_id, 10)

        status, body = self._get("/api/alignment")
        rows = {row["event_id"]: row for row in json.loads(body)}
        row = rows["e-live"]
        self.assertEqual(len(row["live"]), 1)
        self.assertEqual(len(row["replay"]), 1)
        self.assertFalse(row["live"][0]["committed"])
        self.assertFalse(row["replay"][0]["committed"])

    def test_html_page_renders(self):
        status, body = self._get("/")
        self.assertEqual(status, 200)
        self.assertIn("ReplayMQ", body)
        self.assertIn("实时与补账对齐", body)


if __name__ == "__main__":
    unittest.main()
