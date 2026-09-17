"""进程级网络超时保护。

akshare / requests / urllib 在调用方没有显式传 ``timeout`` 时，底层 socket 会一直等
服务端响应。上游数据源偶发卡住时，整条流水线会静默挂死——真实案例：2026-09-16 的
``basic_stock_info`` 卡在某数据源的 443 连接上 30+ 分钟无任何日志。

这里在进程启动时统一安装网络超时，把「无限等待」变成「固定时间内失败」，从而让调用
方要么重试、要么把错误抛到上层日志里，而不是无声地挂住：

- ``install_default_socket_timeout``：给 urllib / http.client 等走 socket 默认值的调用兜底；
- ``install_requests_default_timeout``：requests 在未显式传 timeout 时会把它设成 ``None``，
  等于绕过 socket 默认值，所以必须在 adapter 层再补一次默认值（akshare 大量走 requests）。

只影响未显式设置 timeout 的调用；OpenAI SDK 等自带 timeout 的客户端不受影响。
"""

from __future__ import annotations

import os
import socket

DEFAULT_SOCKET_TIMEOUT_SECONDS = 60.0
ENV_VAR = "NETWORK_SOCKET_TIMEOUT_SECONDS"


def default_socket_timeout_seconds() -> float:
    """读取环境变量配置，非法或非正值时回落到默认 60 秒。"""
    raw = os.getenv(ENV_VAR)
    if raw:
        try:
            value = float(raw)
        except (TypeError, ValueError):
            value = 0.0
        if value > 0:
            return value
    return DEFAULT_SOCKET_TIMEOUT_SECONDS


def install_default_socket_timeout(seconds: float | None = None) -> float:
    """给当前进程安装 socket 默认超时，返回实际生效的秒数。"""
    timeout = default_socket_timeout_seconds() if seconds is None else float(seconds)
    if timeout <= 0:
        raise ValueError(f"socket 超时必须为正数，收到 {timeout}")
    socket.setdefaulttimeout(timeout)
    return timeout


def install_requests_default_timeout(seconds: float | None = None) -> float | None:
    """给 requests 的 HTTPAdapter 补上默认 timeout，返回实际生效的秒数。

    requests 的 ``Session.request(timeout=None)`` 会一路把 ``None`` 传到 urllib3，
    最终 ``sock.settimeout(None)``，即永久阻塞。这里在 ``HTTPAdapter.send`` 上做一层
    包装：调用方没给 timeout 时填默认值，已给则原样透传。
    """
    try:
        from requests.adapters import HTTPAdapter
    except ImportError:  # pragma: no cover - 环境缺 requests
        return None

    timeout = default_socket_timeout_seconds() if seconds is None else float(seconds)
    if timeout <= 0:
        raise ValueError(f"requests 超时必须为正数，收到 {timeout}")

    if getattr(HTTPAdapter, "_default_timeout_patched", False):
        HTTPAdapter._default_timeout_seconds = timeout  # type: ignore[attr-defined]
        return timeout

    original_send = HTTPAdapter.send

    def send_with_default_timeout(self, request, **kwargs):  # type: ignore[no-untyped-def]
        if kwargs.get("timeout") is None:
            kwargs["timeout"] = self._default_timeout_seconds
        return original_send(self, request, **kwargs)

    HTTPAdapter.send = send_with_default_timeout  # type: ignore[method-assign]
    HTTPAdapter._default_timeout_seconds = timeout  # type: ignore[attr-defined]
    HTTPAdapter._default_timeout_patched = True  # type: ignore[attr-defined]
    return timeout


def install_network_timeouts(seconds: float | None = None) -> float:
    """一次安装 socket + requests 两层默认超时，返回生效秒数。"""
    timeout = install_default_socket_timeout(seconds)
    install_requests_default_timeout(timeout)
    return timeout
