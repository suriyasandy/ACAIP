"""
Unit tests for modules/dq_scorer.py
"""
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from modules.dq_scorer import compute_dq_scores, get_dq_summary, WEIGHTS


def _perfect_row() -> dict:
    return {
        "break_id":         "BRK001",
        "asset_class":      "CEQ",
        "owner":            "Alice",
        "approver":         "Bob",
        "status":           "Open",
        "break_age_days":   10,
        "break_amount_gbp": 50000.0,
    }


def _null_governance_row() -> dict:
    return {
        "break_id":         "BRK002",
        "asset_class":      "LnD",
        "owner":            None,
        "approver":         None,
        "status":           None,
        "break_age_days":   5,
        "break_amount_gbp": 10000.0,
    }


class TestComputeDqScores:
    def test_returns_dataframe_with_dq_score(self):
        df = pd.DataFrame([_perfect_row()])
        out = compute_dq_scores(df)
        assert "dq_score" in out.columns

    def test_dq_score_in_range(self):
        rows = [_perfect_row(), _null_governance_row()]
        df   = pd.DataFrame(rows)
        out  = compute_dq_scores(df)
        assert out["dq_score"].between(0, 1).all()

    def test_perfect_row_scores_one(self):
        df  = pd.DataFrame([_perfect_row()])
        out = compute_dq_scores(df)
        assert abs(float(out["dq_score"].iloc[0]) - 1.0) < 1e-6

    def test_null_governance_reduces_score(self):
        perfect = pd.DataFrame([_perfect_row()])
        bad_gov = pd.DataFrame([_null_governance_row()])
        p_score = float(compute_dq_scores(perfect)["dq_score"].iloc[0])
        b_score = float(compute_dq_scores(bad_gov)["dq_score"].iloc[0])
        assert b_score < p_score
        # governance weight is 0.30, so score should be <= 0.70
        assert b_score <= 0.70 + 1e-6

    def test_null_governance_row_scores_below_half(self):
        row = _null_governance_row()
        row["break_amount_gbp"] = 0.0   # also zero amount
        row["asset_class"]      = "UNKNOWN"  # also invalid
        df  = pd.DataFrame([row])
        out = compute_dq_scores(df)
        assert float(out["dq_score"].iloc[0]) < 0.5

    def test_component_columns_added(self):
        df  = pd.DataFrame([_perfect_row()])
        out = compute_dq_scores(df)
        for col in ["dq_governance_ok", "dq_age_valid", "dq_amount_nonzero",
                    "dq_asset_class_valid", "dq_no_duplicate"]:
            assert col in out.columns

    def test_duplicate_break_id_penalised(self):
        row1 = _perfect_row()
        row2 = _perfect_row()
        row2["break_id"] = row1["break_id"]   # duplicate
        df   = pd.DataFrame([row1, row2])
        out  = compute_dq_scores(df)
        # both rows have dq_no_duplicate = 0
        assert (out["dq_no_duplicate"] == 0).all()

    def test_invalid_asset_class_penalised(self):
        row = _perfect_row()
        row["asset_class"] = "INVALID"
        df  = pd.DataFrame([row])
        out = compute_dq_scores(df)
        assert float(out["dq_asset_class_valid"].iloc[0]) == 0.0

    def test_zero_amount_penalised(self):
        row = _perfect_row()
        row["break_amount_gbp"] = 0.0
        df  = pd.DataFrame([row])
        out = compute_dq_scores(df)
        assert float(out["dq_amount_nonzero"].iloc[0]) == 0.0

    def test_weights_sum_to_one(self):
        total = sum(WEIGHTS.values())
        assert abs(total - 1.0) < 1e-9

    def test_does_not_modify_input(self):
        df  = pd.DataFrame([_perfect_row()])
        orig_cols = list(df.columns)
        _   = compute_dq_scores(df)
        assert list(df.columns) == orig_cols


class TestGetDqSummary:
    def test_returns_dict(self):
        df  = pd.DataFrame([_perfect_row(), _null_governance_row()])
        out = compute_dq_scores(df)
        summary = get_dq_summary(out)
        assert isinstance(summary, dict)

    def test_overall_key_present(self):
        df  = pd.DataFrame([_perfect_row()])
        summary = get_dq_summary(compute_dq_scores(df))
        assert "OVERALL" in summary

    def test_asset_class_keys_present(self):
        rows = [_perfect_row(), _null_governance_row()]
        df   = pd.DataFrame(rows)
        summary = get_dq_summary(compute_dq_scores(df))
        assert "CEQ" in summary
        assert "LnD" in summary

    def test_mean_dq_in_range(self):
        df  = pd.DataFrame([_perfect_row(), _null_governance_row()])
        out = compute_dq_scores(df)
        summary = get_dq_summary(out)
        assert 0.0 <= summary["OVERALL"]["mean_dq_score"] <= 1.0

    def test_auto_computes_dq_if_missing(self):
        df = pd.DataFrame([_perfect_row()])
        # dq_score NOT pre-computed
        summary = get_dq_summary(df)
        assert "OVERALL" in summary
