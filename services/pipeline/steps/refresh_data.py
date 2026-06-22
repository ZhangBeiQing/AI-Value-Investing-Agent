"""Step 1: refresh data for the skill pipeline."""

from __future__ import annotations

from typing import Iterable

from services.data_refresh.daily_data_manager import run_manage_daily_data


def run_refresh_data(
    run_date: str,
    signature: str = "",
    *,
    symbols: Iterable[str] | None = None,
    max_workers: int = 4,
    skip_disclosures: bool = False,
) -> None:
    run_manage_daily_data(
        run_date,
        signature=signature,
        symbols=symbols,
        max_workers=max_workers,
        skip_disclosures=skip_disclosures,
    )
