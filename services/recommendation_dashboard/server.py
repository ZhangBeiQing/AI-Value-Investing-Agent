"""Small read-only HTTP server for the recommendation dashboard."""

from __future__ import annotations

import json
import re
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import unquote, urlparse

from core.logging import get_logger
from services.recommendation_dashboard.data import build_dashboard, build_holdings, stock_detail


LOGGER = get_logger("RecommendationDashboard")
STATIC_DIR = Path(__file__).resolve().parent / "static"
STATIC_FILES = {
    "/": ("index.html", "text/html; charset=utf-8"),
    "/app.css": ("app.css", "text/css; charset=utf-8"),
    "/app.js": ("app.js", "text/javascript; charset=utf-8"),
}
SYMBOL_RE = re.compile(r"^[0-9A-Z]+\.(?:SH|SZ|HK|US)$")


def make_handler(data_dir: Path) -> type[BaseHTTPRequestHandler]:
    class DashboardHandler(BaseHTTPRequestHandler):
        def _send(self, status: int, body: bytes, content_type: str) -> None:
            self.send_response(status)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.send_header("X-Content-Type-Options", "nosniff")
            self.send_header("Content-Security-Policy", "default-src 'self'; script-src 'self'; style-src 'self'; img-src 'self' data:; object-src 'none'; base-uri 'none'")
            self.end_headers()
            self.wfile.write(body)

        def _json(self, status: int, payload: object) -> None:
            self._send(status, json.dumps(payload, ensure_ascii=False, allow_nan=False).encode("utf-8"), "application/json; charset=utf-8")

        def do_GET(self) -> None:  # noqa: N802 - stdlib request handler interface
            path = urlparse(self.path).path
            try:
                if path in STATIC_FILES:
                    file_name, content_type = STATIC_FILES[path]
                    self._send(200, (STATIC_DIR / file_name).read_bytes(), content_type)
                    return
                if path == "/api/dashboard":
                    self._json(200, build_dashboard(data_dir))
                    return
                if path == "/api/holdings":
                    self._json(200, build_holdings(data_dir))
                    return
                if path.startswith("/api/stocks/"):
                    symbol = unquote(path.removeprefix("/api/stocks/")).upper()
                    if not SYMBOL_RE.fullmatch(symbol):
                        self._json(400, {"error": "股票代码格式不正确"})
                        return
                    detail = stock_detail(data_dir, symbol)
                    self._json(200, detail) if detail else self._json(404, {"error": "没有该股票的历史分析"})
                    return
                self._json(404, {"error": "页面不存在"})
            except (OSError, ValueError, KeyError, json.JSONDecodeError) as exc:
                LOGGER.exception("仪表盘请求失败: %s", path)
                self._json(500, {"error": f"读取本地数据失败：{exc}"})

        def log_message(self, format: str, *args: object) -> None:
            LOGGER.info("HTTP %s - %s", self.address_string(), format % args)

    return DashboardHandler


def serve(*, host: str, port: int, data_dir: Path) -> None:
    """Serve a local dashboard without changing trading artifacts."""
    if not data_dir.is_dir():
        raise FileNotFoundError(f"数据目录不存在: {data_dir}")
    server = ThreadingHTTPServer((host, port), make_handler(data_dir))
    LOGGER.info("BUY 建议看板已启动: http://%s:%d/", host, port)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        LOGGER.info("BUY 建议看板已停止")
    finally:
        server.server_close()
