"""只读运维页面：实时流量与补账重放的对齐视图。

零依赖，标准库 http.server。页面三块内容：
  1. topic 统计 + gap 水位（最老可用事件 / 已清除区间）
  2. 全部游标与重放会话（状态、独立配额用量、速率、缺口描述）
  3. 对齐表：同一 event_id 的 LIVE 与 REPLAY 两条记录各自的提交情况
"""

from __future__ import annotations

import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from .broker import Broker


def _commit_badge(committed: bool) -> str:
    if committed:
        return '<span class="ok">✓ 已提交</span>'
    return '<span class="pending">… 未提交</span>'


class DashboardHandler(BaseHTTPRequestHandler):
    broker: Broker = None  # type: ignore[assignment]
    topic: str = ""

    def log_message(self, *args):  # 静音
        pass

    def _send(self, code: int, body: bytes, content_type: str):
        self.send_response(code)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _json(self, payload):
        self._send(200, json.dumps(payload, ensure_ascii=False).encode(), "application/json; charset=utf-8")

    def do_GET(self):
        broker: Broker = self.broker
        topic = self.topic
        if self.path.startswith("/api/stats"):
            return self._json({
                "topic": broker.topic_stats(topic),
                "sessions": broker.replay_sessions(),
                "cursors": broker.cursors(topic),
            })
        if self.path.startswith("/api/alignment"):
            return self._json(broker.alignment_view(topic))
        if self.path.startswith("/api/health"):
            return self._json({"ok": True})
        if self.path in ("/", "/index.html"):
            return self._send(200, self._page().encode(), "text/html; charset=utf-8")
        self._send(404, b"not found", "text/plain")

    def _page(self) -> str:
        return PAGE_TEMPLATE.replace("__TOPIC__", self.topic)


PAGE_TEMPLATE = """<!doctype html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<title>ReplayMQ — 实时/补账对齐（topic=__TOPIC__）</title>
<style>
  body { font-family: -apple-system, "Segoe UI", sans-serif; margin: 24px; background: #f6f7f9; color: #1f2430; }
  h1 { font-size: 18px; } h2 { font-size: 15px; margin-top: 28px; }
  table { border-collapse: collapse; width: 100%; background: #fff; font-size: 13px; }
  th, td { border: 1px solid #d9dde5; padding: 6px 10px; text-align: left; vertical-align: top; }
  th { background: #eef1f5; }
  .ok { color: #0a7d33; font-weight: 600; }
  .pending { color: #b26a00; }
  .gap { color: #b3261e; font-weight: 600; }
  .card { background: #fff; border: 1px solid #d9dde5; border-radius: 8px; padding: 14px; margin-bottom: 14px; }
  .mono { font-family: ui-monospace, Menlo, monospace; }
  .live-col { background: #f3f9ff; } .replay-col { background: #fff8f1; }
  small { color: #667085; }
</style>
</head>
<body>
<h1>ReplayMQ 历史重放对齐视图 — topic <span class="mono">__TOPIC__</span>
  <small>（2 秒自动刷新）</small></h1>

<div class="card"><h2 style="margin-top:0">Topic / 游标 / 重放会话</h2>
  <pre id="stats" class="mono">加载中…</pre></div>

<div class="card"><h2 style="margin-top:0">同一事件的实时与补账对齐</h2>
<table>
  <thead><tr>
    <th>event_id</th><th>seq</th><th>event_time</th>
    <th class="live-col">实时游标（LIVE）</th>
    <th class="replay-col">补账游标（REPLAY）</th>
  </tr></thead>
  <tbody id="align"><tr><td colspan="5">加载中…</td></tr></tbody>
</table>
<p><small>两行各自独立显示提交状态：同一事件被两个游标分别投递时，
必须按 cursor_id 区分哪个结果已提交；✓ 来自该游标自己的 ack。</small></p>
</div>

<script>
function badge(c) { return c ? '<span class="ok">✓ 已提交</span>' : '<span class="pending">… 未提交</span>'; }
async function refresh() {
  const [stats, align] = await Promise.all([
    fetch('/api/stats').then(r => r.json()),
    fetch('/api/alignment').then(r => r.json())
  ]);
  document.getElementById('stats').textContent = JSON.stringify(stats, null, 2);

  const rows = align.map(row => {
    const live = row.live.length ? row.live.map(r =>
      `<div>${badge(r.committed)} <small>${r.cursor_id}</small></div>`).join('') : '<small>未投递</small>';
    const replay = row.replay.length ? row.replay.map(r =>
      `<div>${badge(r.committed)} <small>${r.session_id} / ${r.cursor_id}</small></div>`).join('')
      : '<small>未投递</small>';
    return `<tr><td class="mono">${row.event_id}</td><td>${row.seq}</td><td>${row.event_time}</td>
      <td class="live-col">${live}</td><td class="replay-col">${replay}</td></tr>`;
  });
  document.getElementById('align').innerHTML =
    rows.length ? rows.join('') : '<tr><td colspan="5">暂无事件</td></tr>';
}
refresh(); setInterval(refresh, 2000);
</script>
</body></html>"""


def serve(broker: Broker, topic: str, host: str = "127.0.0.1", port: int = 8077) -> ThreadingHTTPServer:
    """启动 dashboard（调用方负责在自己线程里 serve_forever / shutdown）。"""
    handler = type(
        "BoundDashboardHandler",
        (DashboardHandler,),
        {"broker": broker, "topic": topic},
    )
    httpd = ThreadingHTTPServer((host, port), handler)
    return httpd
