"""
Unit tests for modules/feedback_store.py
"""
import sys
import warnings
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from modules.feedback_store import (
    get_feedback_count,
    get_feedback_for_retraining,
    load_feedback,
    save_feedback,
)
from modules.db import get_connection, init_schema


@pytest.fixture
def conn(tmp_path):
    """In-memory DuckDB connection for tests."""
    import duckdb
    c = duckdb.connect(":memory:")
    init_schema(c)
    yield c
    c.close()


class TestSaveFeedback:
    def test_save_returns_true(self, conn):
        result = save_feedback(
            conn, "BRK001", 1, "analyst1", 0.85, "CEQ", "2026-04", False
        )
        assert result is True

    def test_record_persisted(self, conn):
        save_feedback(conn, "BRK002", 0, "analyst1", 0.30, "LnD", "2026-04", False)
        df = conn.execute("SELECT * FROM feedback WHERE break_id = 'BRK002'").df()
        assert len(df) == 1
        assert int(df["analyst_label"].iloc[0]) == 0

    def test_override_flag_stored(self, conn):
        save_feedback(conn, "BRK003", 0, "analyst1", 0.90, "OTC", "2026-04", override_flag=True)
        df = conn.execute("SELECT override_flag FROM feedback WHERE break_id = 'BRK003'").df()
        assert bool(df["override_flag"].iloc[0]) is True

    def test_duplicate_break_id_warns(self, conn):
        save_feedback(conn, "BRK004", 1, "a1", 0.9, "CEQ", "2026-04")
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            save_feedback(conn, "BRK004", 0, "a2", 0.5, "CEQ", "2026-04")
            assert len(w) >= 1
            assert "BRK004" in str(w[0].message)

    def test_schema_columns_match(self, conn):
        save_feedback(conn, "BRK005", 1, "analyst1", 0.80, "MTN", "2026-04")
        df = conn.execute("SELECT * FROM feedback").df()
        required = {"break_id", "analyst_label", "analyst_id",
                    "feedback_timestamp", "model_prediction",
                    "asset_class", "report_month", "override_flag"}
        assert required.issubset(set(df.columns))


class TestLoadFeedback:
    def test_empty_returns_dataframe(self, conn):
        df = load_feedback(conn)
        assert isinstance(df, pd.DataFrame)

    def test_load_returns_saved_records(self, conn):
        save_feedback(conn, "BRK010", 1, "a1", 0.88, "CEQ", "2026-04")
        save_feedback(conn, "BRK011", 0, "a2", 0.22, "LnD", "2026-04")
        df = load_feedback(conn)
        assert len(df) == 2

    def test_columns_present(self, conn):
        save_feedback(conn, "BRK020", 1, "a1", 0.75, "CEQ", "2026-04")
        df = load_feedback(conn)
        assert "break_id" in df.columns
        assert "analyst_label" in df.columns


class TestGetFeedbackCount:
    def test_empty_returns_dict(self, conn):
        result = get_feedback_count(conn)
        assert isinstance(result, dict)

    def test_counts_by_asset_class(self, conn):
        save_feedback(conn, "C1", 1, "a1", 0.9, "CEQ", "2026-04")
        save_feedback(conn, "C2", 1, "a1", 0.8, "CEQ", "2026-04")
        save_feedback(conn, "L1", 0, "a1", 0.2, "LnD", "2026-04")
        counts = get_feedback_count(conn)
        assert counts["CEQ"]["false_break"] == 2
        assert counts["LnD"]["genuine"] == 1

    def test_total_key_present(self, conn):
        save_feedback(conn, "X1", 1, "a1", 0.9, "OTC", "2026-04")
        counts = get_feedback_count(conn)
        assert "TOTAL" in counts
        assert counts["TOTAL"]["total"] >= 1

    def test_filter_by_asset_class(self, conn):
        save_feedback(conn, "M1", 1, "a1", 0.9, "MTN", "2026-04")
        save_feedback(conn, "M2", 0, "a1", 0.1, "MTN", "2026-04")
        save_feedback(conn, "C1", 1, "a1", 0.9, "CEQ", "2026-04")
        counts = get_feedback_count(conn, asset_class="MTN")
        assert counts.get("MTN", {}).get("total", 0) == 2


class TestGetFeedbackForRetraining:
    def test_returns_none_below_threshold(self, conn):
        save_feedback(conn, "A1", 1, "a1", 0.9, "CEQ", "2026-04")
        result = get_feedback_for_retraining(conn, asset_class="CEQ", min_labels=200)
        assert result is None

    def test_returns_dataframe_above_threshold(self, conn):
        for i in range(5):
            save_feedback(conn, f"B{i}", i % 2, "a1", 0.5, "LnD", "2026-04")
        result = get_feedback_for_retraining(conn, asset_class="LnD", min_labels=3)
        assert result is not None
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 5
