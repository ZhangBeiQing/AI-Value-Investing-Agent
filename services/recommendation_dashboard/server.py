"""Local recommendation dashboard with isolated single-stock research jobs."""

from __future__ import annotations

import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, unquote, urlparse

from core.logging import get_logger
from services.recommendation_dashboard.data import build_dashboard, build_holdings, stock_detail
from services.recommendation_dashboard.research import SYMBOL_RE, read_research_package
from services.recommendation_dashboard.jobs import add_stock_to_universe, cancel_job, job_result, latest_jobs, read_job, resolve_stock_name, start_job
from services.recommendation_dashboard.chat import available_models, read_chat, reset_chat, send_message


LOGGER = get_logger("RecommendationDashboard")
STATIC_DIR = Path(__file__).resolve().parent / "static"
STATIC_FILES = {
    "/": ("index.html", "text/html; charset=utf-8"),
    "/app.css": ("app.css", "text/css; charset=utf-8"),
    "/app.js": ("app.js", "text/javascript; charset=utf-8"),
}


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
            parsed = urlparse(self.path)
            path = parsed.path
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
                if path == "/api/chat/models":
                    self._json(200, {"models": available_models()})
                    return
                if path.startswith("/api/chat/"):
                    symbol = unquote(path.removeprefix("/api/chat/")).upper()
                    self._json(200, read_chat(data_dir, symbol))
                    return
                if path == "/api/jobs":
                    self._json(200, {"jobs": latest_jobs(data_dir)})
                    return
                if path.startswith("/api/jobs/"):
                    parts = path.removeprefix("/api/jobs/").split("/")
                    try:
                        item = job_result(data_dir, parts[0]) if len(parts) == 2 and parts[1] == "result" else read_job(data_dir, parts[0]) if len(parts) == 1 else None
                    except ValueError:
                        self._json(400, {"error": "任务编号无效"})
                        return
                    self._json(200, item) if item else self._json(404, {"error": "任务不存在"})
                    return
                if path == "/api/universe/resolve":
                    name = parse_qs(parsed.query).get("name", [""])[0]
                    self._json(200, {"candidates": resolve_stock_name(data_dir, name)})
                    return
                if path.startswith("/api/research/"):
                    symbol = unquote(path.removeprefix("/api/research/")).upper()
                    if not SYMBOL_RE.fullmatch(symbol):
                        self._json(400, {"error": "股票代码格式不正确"})
                        return
                    day = parse_qs(parsed.query).get("date", [None])[0]
                    package = read_research_package(data_dir, symbol, day)
                    self._json(200, package) if package else self._json(404, {"error": "没有该日期的研究包"})
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
            except (OSError, ValueError, KeyError, RuntimeError, json.JSONDecodeError) as exc:
                LOGGER.exception("仪表盘请求失败: %s", path)
                self._json(500, {"error": f"读取本地数据失败：{exc}"})

        def do_POST(self) -> None:  # noqa: N802 - stdlib request handler interface
            if self.client_address[0] not in {"127.0.0.1", "::1"}:
                self._json(403, {"error": "写入接口仅供本机使用"})
                return
            origin = self.headers.get("Origin")
            if origin and origin not in {f"http://127.0.0.1:{self.server.server_port}", f"http://localhost:{self.server.server_port}"}:
                self._json(403, {"error": "跨站请求不被允许"})
                return
            if self.headers.get("Content-Type", "").split(";", 1)[0] != "application/json":
                self._json(415, {"error": "请求必须使用 application/json"})
                return
            try:
                size = int(self.headers.get("Content-Length", "0"))
                if size < 1 or size > 4096:
                    self._json(413, {"error": "请求内容大小不正确"})
                    return
                payload = json.loads(self.rfile.read(size))
                if not isinstance(payload, dict):
                    raise ValueError("请求必须是 JSON 对象")
                if self.path == "/api/jobs":
                    symbol = payload.get("symbol")
                    if not isinstance(symbol, str):
                        raise ValueError("缺少股票代码")
                    self._json(202, start_job(data_dir, symbol))
                    return
                if self.path.startswith("/api/jobs/") and self.path.endswith("/cancel"):
                    job_id = unquote(self.path.removeprefix("/api/jobs/").removesuffix("/cancel"))
                    self._json(200, cancel_job(data_dir, job_id))
                    return
                if self.path.startswith("/api/chat/"):
                    chat_path = self.path.removeprefix("/api/chat/")
                    if chat_path.endswith("/reset"):
                        symbol = unquote(chat_path.removesuffix("/reset")).upper()
                        self._json(200, reset_chat(data_dir, symbol))
                        return
                    symbol = unquote(chat_path).upper()
                    message, model = payload.get("message"), payload.get("model")
                    if not isinstance(message, str) or not isinstance(model, str):
                        raise ValueError("缺少消息或模型")
                    self._json(202, send_message(data_dir, symbol, message, model))
                    return
                if self.path == "/api/universe":
                    symbol, name = payload.get("symbol"), payload.get("name")
                    if not isinstance(symbol, str) or not isinstance(name, str):
                        raise ValueError("缺少股票名称或代码")
                    self._json(200, add_stock_to_universe(data_dir, symbol, name))
                    return
                self._json(404, {"error": "页面不存在"})
            except (ValueError, json.JSONDecodeError) as exc:
                self._json(400, {"error": str(exc)})
            except (OSError, KeyError, RuntimeError) as exc:
                LOGGER.exception("网页写入请求失败: %s", self.path)
                self._json(500, {"error": f"后台操作失败：{exc}"})

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
