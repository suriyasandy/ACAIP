"""
Unit tests for modules/ml_features.py
"""
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from modules.ml_features import (
    ALL_FEATURES,
    ASSET_SPECIFIC_FEATURES,
    build_feature_matrix,
    encode_categoricals,
    get_feature_names,
)


def _make_df(n: int = 50, asset_class: str = "CEQ") -> pd.DataFrame:
    rng = np.random.default_rng(42)
    return pd.DataFrame({
        "break_id":             [f"BRK{i:04d}" for i in range(n)],
        "asset_class":          [asset_class] * n,
        "entity_id":            [f"ENTITY_{i % 5}" for i in range(n)],
        "team_code":            [f"TEAM_{i % 3}" for i in range(n)],
        "rec_name":             [f"REC_{i % 4}" for i in range(n)],
        "source_system":        ["TLM"] * n,
        "break_open_date":      pd.date_range("2026-01-01", periods=n, freq="D"),
        "report_date":          pd.Timestamp("2026-04-01"),
        "last_activity_date":   pd.date_range("2026-02-01", periods=n, freq="D"),
        "break_amount_gbp":     rng.uniform(1000, 2_000_000, n),
        "net_amount_gbp":       rng.uniform(0, 1_000_000, n),
        "gross_amount_gbp":     rng.uniform(1000, 2_000_000, n),
        "break_direction":      ["Dr"] * n,
        "owner":                ["Alice"] * n,
        "approver":             ["Bob"] * n,
        "status":               ["Open"] * n,
        "counterparty":         [f"CPTY_{i % 10}" for i in range(n)],
        "sla_days":             [5] * n,
    })


class TestBuildFeatureMatrix:
    def test_returns_dataframe(self):
        df  = _make_df(50)
        out = build_feature_matrix(df)
        assert isinstance(out, pd.DataFrame)

    def test_index_preserved(self):
        df  = _make_df(20)
        out = build_feature_matrix(df)
        assert list(out.index) == list(df.index)

    def test_feature_count_at_least_42(self):
        df  = _make_df(50)
        out = build_feature_matrix(df)
        assert len(out.columns) >= 42, f"Only {len(out.columns)} features"

    def test_no_nan_in_non_asset_specific_cols(self):
        df = _make_df(50)
        out = build_feature_matrix(df)
        non_asset_specific = [
            c for c in out.columns if c not in ASSET_SPECIFIC_FEATURES
        ]
        for col in non_asset_specific:
            nan_count = int(out[col].isna().sum())
            assert nan_count == 0, f"Column '{col}' has {nan_count} NaN values"

    def test_asset_specific_nan_for_other_classes(self):
        """CEQ-specific features should be NaN for LnD rows."""
        df  = _make_df(10, asset_class="LnD")
        out = build_feature_matrix(df)
        ceq_only = ["div_ex_date_proximity", "corporate_action_flag"]
        for col in ceq_only:
            if col in out.columns:
                assert out[col].isna().all(), \
                    f"CEQ feature '{col}' should be NaN for LnD rows"

    def test_pence_not_passed_raw(self):
        """break_amount_gbp must be in GBP (not millions of pence)."""
        df  = _make_df(10)
        df["break_amount_gbp"] = 1_000_000.0  # £1M per row
        out = build_feature_matrix(df)
        # amount_vs_threshold_ratio should be ≈ 1.0 not ≈ 10000
        ratio = out["amount_vs_threshold_ratio"]
        assert ratio.abs().max() < 100, "Amounts look like pence (too large)"

    def test_all_features_present_in_output(self):
        df  = _make_df(50)
        out = build_feature_matrix(df)
        for feat in ALL_FEATURES:
            assert feat in out.columns, f"Missing feature: {feat}"

    def test_dq_score_input_not_required(self):
        """build_feature_matrix should work without a pre-computed dq_score."""
        df  = _make_df(10)
        assert "dq_score" not in df.columns
        out = build_feature_matrix(df)
        assert len(out) == 10


class TestGetFeatureNames:
    def test_returns_list(self):
        names = get_feature_names()
        assert isinstance(names, list)

    def test_length_at_least_42(self):
        names = get_feature_names()
        assert len(names) >= 42


class TestEncodeCategoricals:
    def test_fit_returns_encoders(self):
        df = _make_df(20)
        _, encoders = encode_categoricals(df, fit=True)
        assert isinstance(encoders, dict)
        assert "entity_id_enc" in encoders

    def test_inference_requires_encoders(self):
        df = _make_df(10)
        with pytest.raises(ValueError, match="encoders"):
            encode_categoricals(df, fit=False, encoders=None)

    def test_inference_uses_fitted_encoders(self):
        df_train = _make_df(30)
        df_infer = _make_df(10)
        df_enc, encoders = encode_categoricals(df_train, fit=True)
        df_infer_enc, _ = encode_categoricals(df_infer, fit=False, encoders=encoders)
        assert "entity_id_enc" in df_infer_enc.columns

    def test_unknown_category_gets_minus_one(self):
        df_train = _make_df(10)
        _, encoders = encode_categoricals(df_train, fit=True)
        df_new = pd.DataFrame({
            "entity_id": ["UNKNOWN_ENTITY"],
            "team_code":  ["UNKNOWN_TEAM"],
            "rec_name":   ["UNKNOWN_REC"],
        })
        df_enc, _ = encode_categoricals(df_new, fit=False, encoders=encoders)
        assert int(df_enc["entity_id_enc"].iloc[0]) == -1
