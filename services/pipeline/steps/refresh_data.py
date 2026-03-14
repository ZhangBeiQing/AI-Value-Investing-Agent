"""Step 1: refresh data for the skill pipeline."""

from __future__ import annotations

from services.data_refresh.daily_data_manager import run_manage_daily_data


def run_refresh_data(run_date: str, signature: str = "") -> None:
    run_manage_daily_data(run_date, signature=signature)

