"""
Unit tests for modules/weak_labeller.py
Verifies R01–R07 rules produce correct labels on synthetic data.
"""
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from modules.weak_labeller import get_label_summary, get_training_set, label_breaks


def _base_row(**overrides) -> dict:
    """Minimal row that matches no rule (R08 → NaN)."""
    row = {
        "break_id":                   "BRK000",
        "asset_class":                "CEQ",
        "break_age_days":             30,
        "rolling_7d_autoclose_rate":  0.10,
        "offsetting_break_exists":    False,
        "is_statistical_outlier":     False,
        "csa_margin_call_flag":       False,
        "coupon_payment_proximity":   20.0,
        "div_ex_date_proximity":      20.0,
        "governance_completeness_score": 0.5,
    }
    row.update(overrides)
    return row


class TestLabelBreaks:
    def test_r08_no_match_returns_nan(self):
        df  = pd.DataFrame([_base_row()])
        out = label_breaks(df)
        assert pd.isna(out["pseudo_label"].iloc[0])
        assert pd.isna(out["label_confidence"].iloc[0])

    def test_r01_high_autoclose_rate(self):
        row = _base_row(rolling_7d_autoclose_rate=0.90)
        df  = pd.DataFrame([row])
        out = label_breaks(df)
        assert float(out["pseudo_label"].iloc[0]) == 1.0
        assert abs(float(out["label_confidence"].iloc[0]) - 0.85) < 1e-6

    def test_r02_offsetting_break(self):
        row = _base_row(offsetting_break_exists=True)
        df  = pd.DataFrame([row])
        out = label_breaks(df)
        assert float(out["pseudo_label"].iloc[0]) == 1.0
        assert abs(float(out["label_confidence"].iloc[0]) - 0.90) < 1e-6

    def test_r03_statistical_outlier_young(self):
        row = _base_row(is_statistical_outlier=True, break_age_days=2)
        df  = pd.DataFrame([row])
        out = label_breaks(df)
        assert float(out["pseudo_label"].iloc[0]) == 1.0
        assert abs(float(out["label_confidence"].iloc[0]) - 0.75) < 1e-6

    def test_r03_outlier_not_fired_for_old_breaks(self):
        row = _base_row(is_statistical_outlier=True, break_age_days=30)
        df  = pd.DataFrame([row])
        out = label_breaks(df)
        assert pd.isna(out["pseudo_label"].iloc[0])

    def test_r04_csa_flag_otc(self):
        row = _base_row(
            asset_class="OTC",
            csa_margin_call_flag=True,
            break_age_days=1,
        )
        df  = pd.DataFrame([row])
        out = label_breaks(df)
        assert float(out["pseudo_label"].iloc[0]) == 1.0
        assert abs(float(out["label_confidence"].iloc[0]) - 0.80) < 1e-6

    def test_r04_not_fired_for_non_otc(self):
        row = _base_row(
            asset_class="CEQ",
            csa_margin_call_flag=True,
            break_age_days=1,
        )
        df  = pd.DataFrame([row])
        out = label_breaks(df)
        assert pd.isna(out["pseudo_label"].iloc[0])

    def test_r05_coupon_proximity_mtn(self):
        row = _base_row(
            asset_class="MTN",
            coupon_payment_proximity=2,
            break_age_days=2,
        )
        df  = pd.DataFrame([row])
        out = label_breaks(df)
        assert float(out["pseudo_label"].iloc[0]) == 1.0
        assert abs(float(out["label_confidence"].iloc[0]) - 0.80) < 1e-6

    def test_r06_div_proximity_ceq(self):
        row = _base_row(
            asset_class="CEQ",
            div_ex_date_proximity=2,
            break_age_days=2,
        )
        df  = pd.DataFrame([row])
        out = label_breaks(df)
        assert float(out["pseudo_label"].iloc[0]) == 1.0
        assert abs(float(out["label_confidence"].iloc[0]) - 0.80) < 1e-6

    def test_r07_genuine_break(self):
        row = _base_row(
            governance_completeness_score=1.0,
            break_age_days=120,
        )
        df  = pd.DataFrame([row])
        out = label_breaks(df)
        assert float(out["pseudo_label"].iloc[0]) == 0.0
        assert abs(float(out["label_confidence"].iloc[0]) - 0.70) < 1e-6

    def test_r01_takes_priority_over_r07(self):
        """R01 fires before R07 — first match wins."""
        row = _base_row(
            rolling_7d_autoclose_rate=0.95,
            governance_completeness_score=1.0,
            break_age_days=120,
        )
        df  = pd.DataFrame([row])
        out = label_breaks(df)
        assert float(out["pseudo_label"].iloc[0]) == 1.0
        assert abs(float(out["label_confidence"].iloc[0]) - 0.85) < 1e-6

    def test_multiple_rows(self):
        rows = [
            _base_row(break_id="R01", rolling_7d_autoclose_rate=0.90),
            _base_row(break_id="R07", governance_completeness_score=1.0, break_age_days=100),
            _base_row(break_id="R08"),  # no match
        ]
        df  = pd.DataFrame(rows)
        out = label_breaks(df)
        assert float(out["pseudo_label"].iloc[0]) == 1.0
        assert float(out["pseudo_label"].iloc[1]) == 0.0
        assert pd.isna(out["pseudo_label"].iloc[2])

    def test_does_not_mutate_input(self):
        df = pd.DataFrame([_base_row()])
        cols_before = list(df.columns)
        label_breaks(df)
        assert list(df.columns) == cols_before


class TestGetTrainingSet:
    def test_excludes_nan_labels(self):
        rows = [
            _base_row(break_id="A", rolling_7d_autoclose_rate=0.95),
            _base_row(break_id="B"),  # R08 → NaN
        ]
        df      = label_breaks(pd.DataFrame(rows))
        train   = get_training_set(df)
        assert "B" not in train["break_id"].values

    def test_excludes_low_confidence(self):
        # R07 has conf=0.70; default min_confidence=0.75 should exclude it
        row = _base_row(governance_completeness_score=1.0, break_age_days=100)
        df  = label_breaks(pd.DataFrame([row]))
        train = get_training_set(df, min_confidence=0.75)
        assert len(train) == 0

    def test_includes_high_confidence(self):
        row = _base_row(rolling_7d_autoclose_rate=0.95)  # R01: conf=0.85
        df  = label_breaks(pd.DataFrame([row]))
        train = get_training_set(df, min_confidence=0.75)
        assert len(train) == 1


class TestGetLabelSummary:
    def test_returns_dict(self):
        df = label_breaks(pd.DataFrame([_base_row()]))
        summary = get_label_summary(df)
        assert isinstance(summary, dict)

    def test_counts_correct(self):
        rows = [
            _base_row(break_id="A", rolling_7d_autoclose_rate=0.95),  # false_break
            _base_row(break_id="B", governance_completeness_score=1.0, break_age_days=100),  # genuine
            _base_row(break_id="C"),  # unlabelled
        ]
        df = label_breaks(pd.DataFrame(rows))
        summary = get_label_summary(df)
        assert summary["false_break"] == 1
        assert summary["genuine"] == 1
        assert summary["unlabelled"] == 1
