"""Config-driven scoring for selection factor snapshots."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Mapping, Sequence

import numpy as np
import pandas as pd
import yaml

from core.logging import get_logger
from services.selection_system.factor_store import FactorStoreConfig, build_factor_store_for_date
from services.selection_system.paths import SelectionSystemPaths
from services.selection_system.store import save_json_file


LOGGER = get_logger("SelectionFactorScoring")
DEFAULT_CONFIG_PATH = Path("configs/selection_system/factor_scoring.yaml")


@dataclass(frozen=True)
class FactorScoringConfig:
    config_path: str | Path = DEFAULT_CONFIG_PATH
    max_staleness_days: int = 10


def build_factor_scores_for_date(
    run_date: str,
    *,
    base_dir: str | Path = "data",
    config: FactorScoringConfig | None = None,
    ensure_factor_store: bool = True,
) -> dict[str, Path]:
    """Build configured factor scores from a raw factor snapshot."""

    config = config or FactorScoringConfig()
    paths = SelectionSystemPaths.from_base_dir(base_dir)
    paths.ensure_directories()
    paths.ensure_run_dir(run_date)

    scoring_config = load_factor_scoring_config(config.config_path)
    snapshot = _load_factor_snapshot(
        run_date,
        paths=paths,
        ensure_factor_store=ensure_factor_store,
        max_staleness_days=config.max_staleness_days,
    )
    scored = score_factor_frame(snapshot, scoring_config)
    scored = _add_horizon_ranks(scored)
    scored = scored.sort_values(["long_score", "short_score"], ascending=False, na_position="last").reset_index(drop=True)
    if "factor_score_rank" not in scored.columns:
        scored.insert(0, "factor_score_rank", range(1, len(scored) + 1))
    scored = _order_factor_score_columns(scored)

    csv_path = paths.run_dir(run_date) / "13_factor_scores.csv"
    json_path = paths.run_dir(run_date) / "13_factor_scores.json"
    scored.to_csv(csv_path, index=False)
    save_json_file(
        json_path,
        {
            "schema_version": 1,
            "run_date": run_date,
            "generated_at": datetime.now().isoformat(),
            "scoring_config_path": str(Path(config.config_path)),
            "summary": {
                "row_count": int(len(scored)),
                "short_pass_count": int(scored.get("short_gate_pass", pd.Series(dtype=bool)).fillna(False).sum()),
                "long_pass_count": int(scored.get("long_gate_pass", pd.Series(dtype=bool)).fillna(False).sum()),
            },
            "columns": list(scored.columns),
            "items": scored.replace({np.nan: None}).to_dict(orient="records"),
        },
    )
    LOGGER.info("配置化因子评分已生成: run_date=%s rows=%d", run_date, len(scored))
    return {
        "factor_scores_csv": csv_path,
        "factor_scores_json": json_path,
    }


def load_factor_scoring_config(config_path: str | Path = DEFAULT_CONFIG_PATH) -> dict[str, Any]:
    path = Path(config_path)
    if not path.is_absolute():
        path = Path.cwd() / path
    with path.open("r", encoding="utf-8") as handle:
        payload = yaml.safe_load(handle) or {}
    if not isinstance(payload, Mapping):
        raise ValueError(f"评分配置格式错误: {path}")
    return dict(payload)


def score_factor_frame(frame: pd.DataFrame, scoring_config: Mapping[str, Any]) -> pd.DataFrame:
    if frame.empty:
        return frame.copy()
    work = frame.copy()
    default_missing_score = float((scoring_config.get("normalization") or {}).get("default_missing_score", 0.5))

    factor_configs = scoring_config.get("factors") or {}
    if not isinstance(factor_configs, Mapping):
        raise ValueError("factor_scoring.yaml 的 factors 必须是 mapping")
    for factor_name, raw_config in factor_configs.items():
        if factor_name not in work.columns:
            continue
        factor_config = raw_config if isinstance(raw_config, Mapping) else {}
        score_col = f"score_{factor_name}"
        work[score_col] = _score_factor_by_type(
            work,
            factor_name,
            factor_config,
            default_missing_score=default_missing_score,
        )

    score_groups = scoring_config.get("score_groups") or {}
    if not isinstance(score_groups, Mapping):
        raise ValueError("factor_scoring.yaml 的 score_groups 必须是 mapping")
    for group_name, raw_weights in score_groups.items():
        if not raw_weights:
            work[group_name] = default_missing_score
            continue
        if not isinstance(raw_weights, Mapping):
            raise ValueError(f"{group_name} 的子权重必须是 mapping")
        work[group_name] = _weighted_group_score(work, raw_weights, default_missing_score=default_missing_score)

    for group_name in ("event_board_score", "money_flow_score"):
        if group_name not in work.columns:
            work[group_name] = default_missing_score

    _apply_horizon_score(work, "short", scoring_config, default_missing_score=default_missing_score)
    _apply_horizon_score(work, "long", scoring_config, default_missing_score=default_missing_score)
    work["combined_score"] = _combined_score(work, scoring_config, default_missing_score=default_missing_score)
    work["exclude_reason"] = _merge_exclude_reasons(work)
    return work


def _order_factor_score_columns(frame: pd.DataFrame) -> pd.DataFrame:
    priority_columns = [
        "factor_score_rank",
        "long_rank",
        "short_rank",
        "factor_rank",
        "date",
        "symbol",
        "stock_name",
        "long_score",
        "short_score",
        "exclude_reason",
    ]
    ordered = [column for column in priority_columns if column in frame.columns]
    remaining = [column for column in frame.columns if column not in ordered]
    return frame[ordered + remaining]


def _add_horizon_ranks(frame: pd.DataFrame) -> pd.DataFrame:
    work = frame.copy()
    for score_column, rank_column in (("short_score", "short_rank"), ("long_score", "long_rank")):
        if score_column not in work.columns:
            continue
        scores = pd.to_numeric(work[score_column], errors="coerce")
        ranks = scores.rank(method="min", ascending=False, na_option="bottom")
        work[rank_column] = ranks.astype("Int64")
        work.loc[scores.isna(), rank_column] = pd.NA
    return work


def _load_factor_snapshot(
    run_date: str,
    *,
    paths: SelectionSystemPaths,
    ensure_factor_store: bool,
    max_staleness_days: int,
) -> pd.DataFrame:
    candidates = [
        paths.run_dir(run_date) / "12_factor_snapshot.csv",
        paths.base_dir / "factor_store" / "by_date" / f"{run_date}.csv",
    ]
    for path in candidates:
        if path.exists():
            return pd.read_csv(path)
    if ensure_factor_store:
        build_factor_store_for_date(
            run_date,
            base_dir=paths.base_dir,
            config=FactorStoreConfig(
                max_staleness_days=max_staleness_days,
                write_parquet=True,
                write_csv=True,
            ),
        )
        for path in candidates:
            if path.exists():
                return pd.read_csv(path)
    raise FileNotFoundError(f"未找到 {run_date} 的 12_factor_snapshot.csv，请先运行 build-factor-store")


def _score_factor(series: pd.Series, factor_config: Mapping[str, Any], *, default_missing_score: float) -> pd.Series:
    direction = str(factor_config.get("direction") or "higher_better")
    values = pd.to_numeric(series, errors="coerce")
    if direction == "range_best":
        return values.map(lambda value: _range_score(value, factor_config, default_missing_score=default_missing_score)).fillna(default_missing_score)

    if bool(factor_config.get("positive_only")):
        values = values.where(values > 0)
    if values.notna().sum() <= 1:
        return pd.Series(default_missing_score, index=series.index)
    if direction == "lower_better":
        ranked = values.rank(pct=True, ascending=False)
    elif direction == "higher_better":
        ranked = values.rank(pct=True, ascending=True)
    else:
        ranked = pd.Series(default_missing_score, index=series.index)
    return ranked.fillna(default_missing_score)


def _score_factor_by_type(
    frame: pd.DataFrame,
    factor_name: str,
    factor_config: Mapping[str, Any],
    *,
    default_missing_score: float,
) -> pd.Series:
    type_configs = factor_config.get("type_configs")
    if not isinstance(type_configs, Mapping):
        return _score_factor(frame[factor_name], factor_config, default_missing_score=default_missing_score)

    stock_type_field = str(factor_config.get("stock_type_field") or "stock_type")
    default_stock_type = str(factor_config.get("default_stock_type") or "growth")
    stock_types = (
        frame[stock_type_field].fillna(default_stock_type).astype(str)
        if stock_type_field in frame.columns
        else pd.Series(default_stock_type, index=frame.index)
    )
    result = pd.Series(default_missing_score, index=frame.index, dtype=float)
    for stock_type in stock_types.unique():
        mask = stock_types == stock_type
        typed_config = _merge_factor_config(factor_config, type_configs.get(stock_type))
        result.loc[mask] = _score_factor(
            frame.loc[mask, factor_name],
            typed_config,
            default_missing_score=default_missing_score,
        )
    return result


def _merge_factor_config(base_config: Mapping[str, Any], raw_override: Any) -> dict[str, Any]:
    merged = {key: value for key, value in base_config.items() if key != "type_configs"}
    if isinstance(raw_override, Mapping):
        merged.update(raw_override)
    return merged


def _range_score(value: Any, factor_config: Mapping[str, Any], *, default_missing_score: float) -> float:
    if value is None or pd.isna(value):
        return default_missing_score
    numeric = float(value)
    ranges = factor_config.get("ranges") or []
    if isinstance(ranges, Sequence):
        for item in ranges:
            if not isinstance(item, Mapping):
                continue
            min_value = item.get("min", -np.inf)
            max_value = item.get("max", np.inf)
            if float(min_value) <= numeric <= float(max_value):
                return float(item.get("score", default_missing_score))
    return float(factor_config.get("default_score", default_missing_score))


def _weighted_group_score(frame: pd.DataFrame, weights: Mapping[str, Any], *, default_missing_score: float) -> pd.Series:
    if "type_weights" in weights:
        return _typed_group_score(frame, weights, default_missing_score=default_missing_score)

    weighted_sum = pd.Series(0.0, index=frame.index)
    total_weight = pd.Series(0.0, index=frame.index)
    for factor_name, raw_weight in weights.items():
        weight = float(raw_weight)
        if weight <= 0:
            continue
        score_col = f"score_{factor_name}"
        if score_col in frame.columns:
            score = pd.to_numeric(frame[score_col], errors="coerce")
        elif factor_name in frame.columns:
            score = pd.to_numeric(frame[factor_name], errors="coerce")
        else:
            continue
        present = score.notna()
        weighted_sum = weighted_sum + score.fillna(0.0) * weight
        total_weight = total_weight + present.astype(float) * weight
    result = weighted_sum / total_weight.replace(0, np.nan)
    return result.fillna(default_missing_score)


def _typed_group_score(frame: pd.DataFrame, group_config: Mapping[str, Any], *, default_missing_score: float) -> pd.Series:
    type_weights = group_config.get("type_weights")
    if not isinstance(type_weights, Mapping):
        return pd.Series(default_missing_score, index=frame.index)

    stock_type_field = str(group_config.get("stock_type_field") or "stock_type")
    default_stock_type = str(group_config.get("default_stock_type") or "growth")
    stock_types = (
        frame[stock_type_field].fillna(default_stock_type).astype(str)
        if stock_type_field in frame.columns
        else pd.Series(default_stock_type, index=frame.index)
    )
    result = pd.Series(default_missing_score, index=frame.index, dtype=float)
    fallback_weights = type_weights.get(default_stock_type, {})
    for stock_type in stock_types.unique():
        mask = stock_types == stock_type
        raw_weights = type_weights.get(stock_type, fallback_weights)
        if not isinstance(raw_weights, Mapping) or not raw_weights:
            result.loc[mask] = default_missing_score
            continue
        result.loc[mask] = _weighted_group_score(frame.loc[mask], raw_weights, default_missing_score=default_missing_score)
    return result


def _apply_horizon_score(
    frame: pd.DataFrame,
    horizon: str,
    scoring_config: Mapping[str, Any],
    *,
    default_missing_score: float,
) -> None:
    horizon_config = scoring_config.get(horizon) or {}
    if not isinstance(horizon_config, Mapping):
        raise ValueError(f"{horizon} 配置必须是 mapping")
    weights = horizon_config.get("main_weights") or {}
    if not isinstance(weights, Mapping):
        raise ValueError(f"{horizon}.main_weights 必须是 mapping")

    raw_score = _weighted_main_score(frame, weights, default_missing_score=default_missing_score)
    pass_mask, reasons = _evaluate_horizon_gates(frame, horizon_config)
    frame[f"{horizon}_score_raw"] = raw_score
    frame[f"{horizon}_gate_pass"] = pass_mask
    frame[f"{horizon}_exclude_reason"] = reasons
    frame[f"{horizon}_score"] = raw_score.where(pass_mask)


def _weighted_main_score(frame: pd.DataFrame, weights: Mapping[str, Any], *, default_missing_score: float) -> pd.Series:
    if "type_weights" in weights:
        return _typed_main_score(frame, weights, default_missing_score=default_missing_score)

    weighted_sum = pd.Series(0.0, index=frame.index)
    total_weight = 0.0
    for group_name, raw_weight in weights.items():
        weight = float(raw_weight)
        if weight <= 0:
            continue
        if group_name in frame.columns:
            score = pd.to_numeric(frame[group_name], errors="coerce").fillna(default_missing_score)
        else:
            score = pd.Series(default_missing_score, index=frame.index)
        weighted_sum = weighted_sum + score * weight
        total_weight += weight
    if total_weight <= 0:
        return pd.Series(default_missing_score, index=frame.index)
    return weighted_sum / total_weight


def _typed_main_score(frame: pd.DataFrame, weights: Mapping[str, Any], *, default_missing_score: float) -> pd.Series:
    type_weights = weights.get("type_weights")
    if not isinstance(type_weights, Mapping):
        return pd.Series(default_missing_score, index=frame.index)

    stock_type_field = str(weights.get("stock_type_field") or "stock_type")
    default_stock_type = str(weights.get("default_stock_type") or "growth")
    stock_types = (
        frame[stock_type_field].fillna(default_stock_type).astype(str)
        if stock_type_field in frame.columns
        else pd.Series(default_stock_type, index=frame.index)
    )
    result = pd.Series(default_missing_score, index=frame.index, dtype=float)
    fallback_weights = type_weights.get(default_stock_type, {})
    for stock_type in stock_types.unique():
        mask = stock_types == stock_type
        raw_weights = type_weights.get(stock_type, fallback_weights)
        if not isinstance(raw_weights, Mapping) or not raw_weights:
            result.loc[mask] = default_missing_score
            continue
        result.loc[mask] = _weighted_main_score(frame.loc[mask], raw_weights, default_missing_score=default_missing_score)
    return result


def _evaluate_gates(frame: pd.DataFrame, gates: Any) -> tuple[pd.Series, pd.Series]:
    pass_mask = pd.Series(True, index=frame.index)
    reasons = pd.Series("", index=frame.index, dtype=object)
    if not isinstance(gates, Sequence):
        return pass_mask, reasons
    for raw_gate in gates:
        if not isinstance(raw_gate, Mapping):
            continue
        field = str(raw_gate.get("field") or "")
        if not field or field not in frame.columns:
            if bool(raw_gate.get("allow_missing")):
                continue
            fail = pd.Series(True, index=frame.index)
        else:
            values = pd.to_numeric(frame[field], errors="coerce")
            fail = values.isna() & (not bool(raw_gate.get("allow_missing")))
            if "min" in raw_gate:
                fail = fail | (values < float(raw_gate["min"]))
            if "max" in raw_gate:
                fail = fail | (values > float(raw_gate["max"]))
            if bool(raw_gate.get("positive")):
                fail = fail | (values <= 0)
        reason = str(raw_gate.get("reason") or field or "gate_failed")
        pass_mask = pass_mask & ~fail
        reasons = _append_reason(reasons, fail, reason)
    return pass_mask, reasons


def _evaluate_horizon_gates(frame: pd.DataFrame, horizon_config: Mapping[str, Any]) -> tuple[pd.Series, pd.Series]:
    type_gates = horizon_config.get("type_gates")
    if not isinstance(type_gates, Mapping):
        return _evaluate_gates(frame, horizon_config.get("gates") or [])

    stock_type_field = str(horizon_config.get("stock_type_field") or "stock_type")
    default_stock_type = str(horizon_config.get("default_stock_type") or "growth")
    stock_types = (
        frame[stock_type_field].fillna(default_stock_type).astype(str)
        if stock_type_field in frame.columns
        else pd.Series(default_stock_type, index=frame.index)
    )
    pass_mask = pd.Series(True, index=frame.index)
    reasons = pd.Series("", index=frame.index, dtype=object)
    fallback_gates = horizon_config.get("gates") or []

    for stock_type in stock_types.unique():
        mask = stock_types == stock_type
        gates = type_gates.get(stock_type, fallback_gates)
        sub_pass, sub_reasons = _evaluate_gates(frame.loc[mask], gates)
        pass_mask.loc[mask] = sub_pass
        reasons.loc[mask] = sub_reasons
    return pass_mask, reasons


def _append_reason(reasons: pd.Series, fail: pd.Series, reason: str) -> pd.Series:
    updated = reasons.copy()
    fail = fail.fillna(False)
    existing = updated[fail].fillna("").astype(str)
    updated.loc[fail] = existing.map(lambda value: f"{value};{reason}" if value else reason)
    return updated


def _combined_score(frame: pd.DataFrame, scoring_config: Mapping[str, Any], *, default_missing_score: float) -> pd.Series:
    weights = scoring_config.get("combined") or {"short_score": 0.5, "long_score": 0.5}
    if not isinstance(weights, Mapping):
        weights = {"short_score": 0.5, "long_score": 0.5}
    weighted_sum = pd.Series(0.0, index=frame.index)
    total_weight = 0.0
    for field, raw_weight in weights.items():
        weight = float(raw_weight)
        if weight <= 0:
            continue
        if field in frame.columns:
            score = pd.to_numeric(frame[field], errors="coerce").fillna(default_missing_score)
        else:
            score = pd.Series(default_missing_score, index=frame.index)
        weighted_sum = weighted_sum + score * weight
        total_weight += weight
    if total_weight <= 0:
        return pd.Series(default_missing_score, index=frame.index)
    return weighted_sum / total_weight


def _merge_exclude_reasons(frame: pd.DataFrame) -> pd.Series:
    short_reason = frame.get("short_exclude_reason", pd.Series("", index=frame.index)).fillna("").astype(str)
    long_reason = frame.get("long_exclude_reason", pd.Series("", index=frame.index)).fillna("").astype(str)
    merged = []
    for short_value, long_value in zip(short_reason, long_reason):
        parts = []
        if short_value:
            parts.append(f"short:{short_value}")
        if long_value:
            parts.append(f"long:{long_value}")
        merged.append("|".join(parts))
    return pd.Series(merged, index=frame.index)


__all__ = [
    "DEFAULT_CONFIG_PATH",
    "FactorScoringConfig",
    "build_factor_scores_for_date",
    "load_factor_scoring_config",
    "score_factor_frame",
]
