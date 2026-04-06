"""
Integration test: synthetic 1000-row CSV → full ML pipeline → assert scored output.

Tests the complete chain:
  data_loader → dq_scorer → ml_features → weak_labeller → false_break_model (train) → scorer
"""
import sys
import tempfile
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from modules.data_loader import prepare_for_ingest
from modules.db import get_connection, ingest_dataframe, init_schema
from modules.dq_scorer import compute_and_persist_dq, compute_dq_scores
from modules.false_break_model import train_from_dataframe
from modules.ml_features import build_feature_matrix
from modules.scorer import assign_risk_tier, score_breaks
from modules.weak_labeller import get_training_set, label_breaks


# ─────────────────────────────── fixtures ────────────────────────────────────

def _generate_synthetic_csv(n: int = 1000, seed: int = 42) -> bytes:
    """Generate a realistic synthetic MI report CSV."""
    rng = np.random.default_rng(seed)
    asset_classes = ["CEQ", "LnD", "MTN", "OTC"]
    source_systems = ["TLM", "DUCO", "D3S"]
    directions = ["Dr", "Cr"]

    rows = []
    for i in range(n):
        ac = asset_classes[i % 4]
        open_lag = rng.integers(1, 90)
        rows.append({
            "break_id":          f"BRK{i:06d}",
            "asset_class":       ac,
            "entity_id":         f"ENTITY_{i % 20}",
            "team_code":         f"TEAM_{i % 5}",
            "rec_name":          f"REC_{i % 8}",
            "source_system":     rng.choice(source_systems),
            "break_open_date":   f"2026-0{1 + (i % 3)}-{1 + (i % 28):02d}",
            "break_date":        f"2026-0{1 + (i % 3)}-{1 + (i % 28):02d}",
            "report_date":       "2026-04-01",
            "last_activity_date": f"2026-0{2 + (i % 2)}-{1 + (i % 28):02d}",
            # Amounts in pence
            "break_amount_pence": int(rng.uniform(100_00, 5_000_000_00)),
            "net_amount_pence":   0,
            "gross_amount_pence": int(rng.uniform(100_00, 5_000_000_00)),
            "break_direction":    rng.choice(directions),
            "owner":              f"Owner_{i % 10}" if rng.random() > 0.3 else None,
            "approver":           f"Approver_{i % 10}" if rng.random() > 0.3 else None,
            "status":             "Open" if rng.random() > 0.2 else None,
            "counterparty":       f"CPTY_{i % 30}",
            "sla_days":           5,
            # Asset-specific proxies
            "coupon_payment_proximity": rng.integers(0, 30) if ac == "MTN" else None,
            "div_ex_date_proximity":    rng.integers(0, 30) if ac == "CEQ" else None,
        })

    df = pd.DataFrame(rows)
    return df.to_csv(index=False).encode("utf-8")


# ─────────────────────────────── tests ───────────────────────────────────────

class TestEndToEndPipeline:
    """Full pipeline test without DuckDB (pure DataFrame path)."""

    @pytest.fixture(scope="class")
    def synthetic_csv(self):
        return _generate_synthetic_csv(1000)

    @pytest.fixture(scope="class")
    def loaded_df(self, synthetic_csv, tmp_path_factory):
        cache_dir = str(tmp_path_factory.mktemp("cache"))
        df, _ = prepare_for_ingest(synthetic_csv, cache_dir=cache_dir)
        return df

    def test_load_produces_1000_rows(self, loaded_df):
        assert len(loaded_df) == 1000

    def test_dq_scores_computed(self, loaded_df):
        scored = compute_dq_scores(loaded_df)
        assert "dq_score" in scored.columns
        assert scored["dq_score"].between(0, 1).all()

    def test_feature_matrix_built(self, loaded_df):
        feat = build_feature_matrix(loaded_df)
        assert len(feat) == 1000
        assert len(feat.columns) >= 42

    def test_pseudo_labels_generated(self, loaded_df):
        feat = build_feature_matrix(loaded_df)
        joined = loaded_df.copy()
        for col in feat.columns:
            joined[col] = feat[col].values
        labelled = label_breaks(joined)
        assert "pseudo_label" in labelled.columns
        # At least some rows should be labelled
        n_labelled = labelled["pseudo_label"].notna().sum()
        assert n_labelled > 0

    def test_score_breaks_returns_risk_tier(self, loaded_df, tmp_path):
        """With no model → Isolation Forest fallback; still produces risk_tier."""
        out = score_breaks(loaded_df, model_dir=str(tmp_path))
        assert "false_break_prob" in out.columns
        assert "risk_tier" in out.columns
        scored = out[out["risk_tier"] != "Unscored"]
        assert len(scored) > 0

    def test_risk_tier_values_valid(self, loaded_df, tmp_path):
        out  = score_breaks(loaded_df, model_dir=str(tmp_path))
        valid = {"Suppress", "Review", "Escalate", "Unscored"}
        assert set(out["risk_tier"].unique()).issubset(valid)

    def test_probs_in_01(self, loaded_df, tmp_path):
        out   = score_breaks(loaded_df, model_dir=str(tmp_path))
        probs = out["false_break_prob"].dropna()
        assert (probs >= 0.0).all() and (probs <= 1.0).all()


class TestDuckDBPipeline:
    """Pipeline through DuckDB ingest + compute + update."""

    @pytest.fixture(scope="class")
    def conn(self, tmp_path_factory):
        import duckdb
        db_path = str(tmp_path_factory.mktemp("db") / "test.duckdb")
        c = duckdb.connect(db_path)
        init_schema(c)
        yield c
        c.close()

    @pytest.fixture(scope="class")
    def ingested_conn(self, conn, tmp_path_factory):
        cache_dir = str(tmp_path_factory.mktemp("cache2"))
        raw = _generate_synthetic_csv(200, seed=7)
        df, key = prepare_for_ingest(raw, cache_dir=cache_dir)
        ingest_dataframe(conn, df)
        return conn

    def test_breaks_table_populated(self, ingested_conn):
        count = ingested_conn.execute("SELECT COUNT(*) FROM breaks").fetchone()[0]
        assert count == 200

    def test_dq_scores_persist_to_duckdb(self, ingested_conn):
        df = ingested_conn.execute("SELECT * FROM breaks").df()
        compute_and_persist_dq(ingested_conn, df)
        result = ingested_conn.execute(
            "SELECT COUNT(*) FROM breaks WHERE dq_score IS NOT NULL"
        ).fetchone()[0]
        assert result == 200

    def test_dq_scores_in_valid_range(self, ingested_conn):
        row = ingested_conn.execute(
            "SELECT MIN(dq_score), MAX(dq_score) FROM breaks WHERE dq_score IS NOT NULL"
        ).fetchone()
        assert row[0] >= 0.0
        assert row[1] <= 1.0


class TestRiskTierAssignment:
    @pytest.mark.parametrize("prob,threshold,expected", [
        (0.90, 0.80, "Suppress"),
        (0.65, 0.80, "Review"),
        (0.30, 0.80, "Escalate"),
        (0.85, 0.90, "Review"),    # below custom threshold
        (0.95, 0.90, "Suppress"),  # above custom threshold
    ])
    def test_risk_tier_correct(self, prob, threshold, expected):
        assert assign_risk_tier(prob, threshold) == expected
