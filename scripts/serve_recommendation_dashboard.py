"""CLI entrypoint for the read-only fixed-tracked BUY dashboard."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from services.recommendation_dashboard.server import serve  # noqa: E402


def main() -> None:
    parser = argparse.ArgumentParser(description="查看 fixed_tracked 历史 BUY 建议效果")
    parser.add_argument("--host", default="127.0.0.1", help="监听地址，默认仅本机可访问")
    parser.add_argument("--port", type=int, default=8765, help="监听端口")
    parser.add_argument("--data-dir", type=Path, default=PROJECT_ROOT / "data", help="项目数据目录")
    args = parser.parse_args()
    serve(host=args.host, port=args.port, data_dir=args.data_dir.resolve())


if __name__ == "__main__":
    main()
