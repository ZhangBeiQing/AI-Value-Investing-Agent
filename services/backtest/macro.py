"""Validate experiment-local historical macro summaries before input generation."""

from __future__ import annotations

from typing import Any

from services.backtest.experiment import BacktestExperiment


REQUIRED_SECTIONS = (
    "今日变化", "市场与利率面板", "政策与数据", "持续性风险", "资产映射与明日关注",
)


def inspect_experiment_macro(
    experiment: BacktestExperiment, run_date: str,
) -> tuple[dict[str, Any], str | None]:
    """Reuse by default; explicit regeneration fails closed until a valid draft exists."""
    policy = experiment.payload.get("macro_summary_policy", "reuse")
    path = experiment.root / "macro_economy" / f"{run_date.replace('-', '')}.md"
    if policy == "reuse" and not path.exists():
        files = sorted(
            candidate for candidate in (experiment.context.source_data_root / "macro_economy").glob("*.md")
            if candidate.stem.isdigit() and len(candidate.stem) == 8
            and candidate.stem <= run_date.replace("-", "")
            and candidate.stat().st_size > 0
        )
        if files:
            return {"status": "ready", "policy": policy, "source_path": str(files[-1])}, files[-1].read_text(encoding="utf-8")
    issues: list[str] = []
    text = path.read_text(encoding="utf-8") if path.exists() else ""
    if not text.strip():
        issues.append("缺少实验隔离宏观总结")
    else:
        if run_date not in text:
            issues.append("正文缺少分析日期")
        if sum(not char.isspace() for char in text) > 3000:
            issues.append("全文非空白字符超过 3000")
        if any(f"## {section}" not in text for section in REQUIRED_SECTIONS):
            issues.append("缺少宏观报告契约要求的五节")
    return {
        "status": "needs_macro_research" if issues else "ready",
        "policy": policy,
        "output_path": str(path),
        "issues": issues,
        "knowledge_cutoff": f"{run_date}T23:59:59+08:00",
    }, text if not issues else None
