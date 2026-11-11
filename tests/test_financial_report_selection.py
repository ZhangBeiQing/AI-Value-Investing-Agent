from datetime import datetime
from pathlib import Path

import pytest

from services.research.financial_report import _select_reports


@pytest.mark.parametrize(
    ("names", "expected"),
    [
        (["20261030.md", "20261030_manual.md"], "20261030.md"),
        (["20261030_manual.md", "20261030.md"], "20261030.md"),
        (["20261030_manual.md"], "20261030_manual.md"),
        (["20261029.md", "20261030_manual.md"], "20261030_manual.md"),
    ],
)
def test_select_reports_same_day_prefers_canonical_but_preserves_manual_fallback(names, expected):
    selected, _ = _select_reports(
        [Path(name) for name in names], datetime(2026, 11, 10),
    )

    assert selected.path.name == expected
