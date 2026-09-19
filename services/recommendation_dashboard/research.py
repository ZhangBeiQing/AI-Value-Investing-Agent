"""Read stock research packages without exposing arbitrary filesystem paths."""

from __future__ import annotations

import re
from pathlib import Path


SYMBOL_RE = re.compile(r"^[0-9A-Z]+\.(?:SH|SZ|HK|US)$")
DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def research_packages(data_dir: Path, symbol: str) -> list[dict[str, str]]:
    """List matching packages from official daily runs, newest first."""
    if not SYMBOL_RE.fullmatch(symbol):
        raise ValueError("股票代码格式不正确")
    runs_dir = data_dir / "skill_runs"
    if not runs_dir.is_dir():
        return []
    packages: list[dict[str, str]] = []
    for run_dir in runs_dir.iterdir():
        if not run_dir.is_dir() or not DATE_RE.fullmatch(run_dir.name):
            continue
        if not run_dir.resolve().is_relative_to(runs_dir.resolve()):
            continue
        research_dir = run_dir / "fixed_tracked" / "04_stock_research"
        if not research_dir.is_dir():
            continue
        suffix = f"_{symbol}_{run_dir.name}_research.md"
        for path in research_dir.iterdir():
            if path.is_file() and path.resolve().is_relative_to(research_dir.resolve()) and path.name.endswith(suffix):
                packages.append({"date": run_dir.name, "name": path.name})
    return sorted(packages, key=lambda item: item["date"], reverse=True)


def read_research_package(data_dir: Path, symbol: str, day: str | None = None) -> dict[str, str] | None:
    packages = research_packages(data_dir, symbol)
    selected = next((item for item in packages if day is None or item["date"] == day), None)
    if selected is None:
        return None
    research_dir = data_dir / "skill_runs" / selected["date"] / "fixed_tracked" / "04_stock_research"
    path = research_dir / selected["name"]
    if not path.resolve().is_relative_to(research_dir.resolve()):
        raise ValueError("研究包路径超出允许目录")
    return {**selected, "content": path.read_text(encoding="utf-8")}
