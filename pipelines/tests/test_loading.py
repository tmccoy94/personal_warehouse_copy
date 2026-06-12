"""Unit tests for pipelines.loading helpers."""
from datetime import date
from unittest.mock import patch

import pytest

from pipelines.loading import assert_snapshot_db_matches_partition


class _FakeConn:
    """Stand-in for an ibm_db connection; only used as a passthrough."""


def test_snapshot_db_matches_partition_passes_on_exact_match():
    # MONTH_END_2 holding May 2026 data — POSTING_DATE = 2026-05-31
    with patch("pipelines.loading.db2_fetch_all",
               return_value=[{"posting_date": date(2026, 5, 31)}]):
        assert_snapshot_db_matches_partition(_FakeConn(), date(2026, 5, 1))


def test_snapshot_db_matches_partition_raises_when_db_is_behind():
    # QUARTER_END still holds Mar 2026 — user tries to load June 2026 in mid-June.
    # This is the exact bug that produced the bad 2026-06-30 row in person_snapshot_monthly.
    with patch("pipelines.loading.db2_fetch_all",
               return_value=[{"posting_date": date(2026, 3, 31)}]):
        with pytest.raises(RuntimeError, match="does not currently hold data"):
            assert_snapshot_db_matches_partition(_FakeConn(), date(2026, 6, 1))


def test_snapshot_db_matches_partition_raises_when_db_has_rolled_forward():
    # Trying to re-load Feb 2026 from MONTH_END_2 after the Aug 2026 refresh.
    with patch("pipelines.loading.db2_fetch_all",
               return_value=[{"posting_date": date(2026, 8, 31)}]):
        with pytest.raises(RuntimeError, match="does not currently hold data"):
            assert_snapshot_db_matches_partition(_FakeConn(), date(2026, 2, 1))


def test_snapshot_db_matches_partition_accepts_iso_string_dates():
    # Some DB2 drivers return DATE columns as ISO strings; helper must coerce.
    with patch("pipelines.loading.db2_fetch_all",
               return_value=[{"posting_date": "2026-05-31"}]):
        assert_snapshot_db_matches_partition(_FakeConn(), date(2026, 5, 1))


def test_snapshot_db_matches_partition_raises_when_env_returns_no_rows():
    with patch("pipelines.loading.db2_fetch_all", return_value=[]):
        with pytest.raises(RuntimeError, match="CORE.ENV returned zero rows"):
            assert_snapshot_db_matches_partition(_FakeConn(), date(2026, 5, 1))


def test_snapshot_db_matches_partition_handles_february_eom():
    # Catch year-end + non-leap February edge cases in last_day_of_month.
    with patch("pipelines.loading.db2_fetch_all",
               return_value=[{"posting_date": date(2026, 2, 28)}]):
        assert_snapshot_db_matches_partition(_FakeConn(), date(2026, 2, 1))


def test_snapshot_db_matches_partition_handles_december_eom():
    with patch("pipelines.loading.db2_fetch_all",
               return_value=[{"posting_date": date(2026, 12, 31)}]):
        assert_snapshot_db_matches_partition(_FakeConn(), date(2026, 12, 1))
