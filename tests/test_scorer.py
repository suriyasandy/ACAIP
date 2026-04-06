"""
Unit tests for modules/scorer.py
"""
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from modules.scorer import assign_risk_tier, compute_suppression_stats, score_breaks


class TestAssignRiskTier:
    def test_suppress_above_threshold(self):
        assert assign_risk_tier(0.90, threshold=0.80) == "Suppress"

    def test_suppress_just_above(self):
        assert assign_risk_tier(0.801, threshold=0.80) == "Suppress"

    def test_review_at_threshold(self):
        # prob exactly at threshold → Review (not Suppress — rule is > threshold)
        assert assign_risk_tier(0.80, threshold=0.80) == "Review"

    def test_review_between(self):
        assert assign_risk_tier(0.65, threshold=0.80) == "Review"

    def test_review_at_lower_bound(self):
        assert assign_risk_tier(0.50, threshold=0.80) == "Review"

    def test_escalate_below_lower_bound(self):
        assert assign_risk_tier(0.30, threshold=0.80) == "Escalate"

    def test_escalate_just_below(self):
        assert assign_risk_tier(0.499, threshold=0.80) == "Escalate"

    def test_custom_threshold_respected(self):
        assert assign_risk_tier(0.75, threshold=0.70) == "Suppress"
        assert assign_risk_tier(0.75, threshold=0.80) == "Review"

    def test_zero_prob_escalate(self):
        assert assign_risk_tier(0.0) == "Escalate"

    def test_one_prob_suppress(self):
        assert assign_risk_tier(1.0) == "Suppress"


class TestScoreBreaks:
    """
    When no model files are present, score_breaks falls back to Isolation Forest.
    We test with a tmp model_dir that has no model files.
    """

    def _make_df(self, n: int = 20) -> pd.DataFrame:
        rng = np.random.default_rng(0)
        return pd.DataFrame({
            "break_id":         [f"BRK{i:04d}" for i in range(n)],
            "asset_class":      ["CEQ"] * 10 + ["LnD"] * 10,
            "entity_id":        [f"E{i%5}" for i in range(n)],
            "team_code":        ["TEAM_A"] * n,
            "rec_name":         ["REC_1"] * n,
            "source_system":    ["TLM"] * n,
            "break_open_date":  pd.date_range("2026-01-01", periods=n, freq="D"),
            "report_date":      pd.Timestamp("2026-04-01"),
            "last_activity_date": pd.date_range("2026-02-01", periods=n, freq="D"),
            "break_amount_gbp": rng.uniform(1000, 2_000_000, n),
            "owner":            ["Alice"] * n,
            "approver":         ["Bob"] * n,
            "status":           ["Open"] * n,
            "sla_days":         [5] * n,
        })

    def test_returns_dataframe(self, tmp_path):
        df  = self._make_df()
        out = score_breaks(df, model_dir=str(tmp_path))
        assert isinstance(out, pd.DataFrame)

    def test_false_break_prob_column_added(self, tmp_path):
        df  = self._make_df()
        out = score_breaks(df, model_dir=str(tmp_path))
        assert "false_break_prob" in out.columns

    def test_risk_tier_column_added(self, tmp_path):
        df  = self._make_df()
        out = score_breaks(df, model_dir=str(tmp_path))
        assert "risk_tier" in out.columns

    def test_risk_tier_values_valid(self, tmp_path):
        df  = self._make_df()
        out = score_breaks(df, model_dir=str(tmp_path))
        valid = {"Suppress", "Review", "Escalate", "Unscored"}
        assert set(out["risk_tier"].unique()).issubset(valid)

    def test_probs_in_range(self, tmp_path):
        df  = self._make_df()
        out = score_breaks(df, model_dir=str(tmp_path))
        probs = out["false_break_prob"].dropna()
        assert (probs >= 0.0).all() and (probs <= 1.0).all()

    def test_threshold_param_respected(self, tmp_path):
        """With low threshold all scored rows should be Suppress."""
        df  = self._make_df()
        out = score_breaks(df, model_dir=str(tmp_path), threshold=0.01)
        scored = out[out["risk_tier"] != "Unscored"]
        if len(scored) > 0:
            assert (scored["risk_tier"] == "Suppress").all()

    def test_handles_missing_asset_class(self, tmp_path):
        df = pd.DataFrame({"break_id": ["X"], "break_amount_gbp": [100.0]})
        out = score_breaks(df, model_dir=str(tmp_path))
        assert "risk_tier" in out.columns
        assert out["risk_tier"].iloc[0] == "Unscored"


class TestComputeSuppressionStats:
    def test_empty_df(self):
        df = pd.DataFrame({"risk_tier": []})
        stats = compute_suppression_stats(df)
        assert stats["total"] == 0

    def test_all_suppressed(self):
        df = pd.DataFrame({
            "risk_tier":        ["Suppress"] * 5,
            "break_amount_gbp": [10000.0] * 5,
        })
        stats = compute_suppression_stats(df)
        assert stats["suppressed"] == 5
        assert abs(stats["suppressed_pct"] - 100.0) < 1e-6

    def test_mixed_tiers(self):
        df = pd.DataFrame({
            "risk_tier":        ["Suppress", "Review", "Escalate", "Suppress"],
            "break_amount_gbp": [10000.0, 5000.0, 20000.0, 8000.0],
        })
        stats = compute_suppression_stats(df)
        assert stats["suppressed"] == 2
        assert stats["review"]     == 1
        assert stats["escalate"]   == 1
        assert abs(stats["value_suppressed"] - 18000.0) < 1e-2

    def test_no_risk_tier_column(self):
        df = pd.DataFrame({"break_id": ["A"]})
        stats = compute_suppression_stats(df)
        assert stats == {}
