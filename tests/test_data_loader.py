"""
Unit tests for modules/data_loader.py
"""
import hashlib
import io
import os
import sys
import tempfile
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from modules.data_loader import (
    PENCE_COLUMNS,
    convert_pence,
    get_cache_key,
    load_mi_report,
    load_or_cache,
    prepare_for_ingest,
)


def _make_csv_bytes(n_rows: int = 10) -> bytes:
    """Generate a minimal valid MI report CSV."""
    lines = ["break_id,asset_class,entity_id,team_code,rec_name,source_system,"
             "break_open_date,break_date,report_date,last_activity_date,"
             "break_amount_pence,net_amount_pence,gross_amount_pence,"
             "break_direction,owner,approver,status,counterparty,sla_days"]
    for i in range(n_rows):
        lines.append(
            f"BRK{i:05d},CEQ,ENTITY_{i % 3},TEAM_A,REC_{i % 5},TLM,"
            f"2026-03-01,2026-03-01,2026-04-01,2026-03-15,"
            f"{(i + 1) * 150000},0,0,"
            f"Dr,Owner_{i},Approver_{i},Open,CPTY_{i % 10},5"
        )
    return "\n".join(lines).encode("utf-8")


class TestGetCacheKey:
    def test_deterministic(self):
        raw = b"hello world"
        assert get_cache_key(raw) == get_cache_key(raw)

    def test_sha256_hex_length(self):
        key = get_cache_key(b"test bytes")
        assert len(key) == 64  # SHA-256 hex digest is always 64 chars

    def test_different_bytes_different_key(self):
        assert get_cache_key(b"aaa") != get_cache_key(b"bbb")

    def test_matches_hashlib(self):
        raw = b"some file content"
        expected = hashlib.sha256(raw).hexdigest()
        assert get_cache_key(raw) == expected


class TestLoadMiReport:
    def test_returns_dataframe(self):
        raw = _make_csv_bytes(5)
        df = load_mi_report(raw)
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 5

    def test_break_id_column_present(self):
        raw = _make_csv_bytes(3)
        df = load_mi_report(raw)
        assert "break_id" in df.columns

    def test_date_columns_parsed(self):
        raw = _make_csv_bytes(3)
        df = load_mi_report(raw)
        assert pd.api.types.is_datetime64_any_dtype(df["report_date"])

    def test_missing_break_id_raises(self):
        bad_csv = b"col_a,col_b\n1,2\n3,4\n"
        with pytest.raises(ValueError, match="break_id"):
            load_mi_report(bad_csv)

    def test_alias_columns_normalised(self):
        csv = b"BreakID,AssetClass\nBRK001,CEQ\n"
        df = load_mi_report(csv)
        assert "break_id" in df.columns
        assert "asset_class" in df.columns


class TestConvertPence:
    def test_pence_to_gbp(self):
        df = pd.DataFrame({"break_amount_pence": [100000, 250000]})
        out = convert_pence(df, ["break_amount_pence"])
        assert "break_amount_gbp" in out.columns
        assert abs(out["break_amount_gbp"].iloc[0] - 1000.0) < 1e-6
        assert abs(out["break_amount_gbp"].iloc[1] - 2500.0) < 1e-6

    def test_originals_preserved(self):
        df = pd.DataFrame({"break_amount_pence": [100000]})
        out = convert_pence(df, ["break_amount_pence"])
        assert "break_amount_pence" in out.columns
        assert out["break_amount_pence"].iloc[0] == 100000

    def test_missing_col_ignored(self):
        df = pd.DataFrame({"other_col": [1, 2]})
        out = convert_pence(df, ["break_amount_pence"])
        assert "break_amount_gbp" not in out.columns

    def test_non_amount_cols_not_touched(self):
        df = pd.DataFrame({
            "break_amount_pence": [10000],
            "break_age_days": [5],
        })
        out = convert_pence(df, ["break_amount_pence"])
        assert out["break_age_days"].iloc[0] == 5


class TestLoadOrCache:
    def test_cache_creates_parquet(self, tmp_path):
        raw = _make_csv_bytes(5)
        df = load_or_cache(raw, cache_dir=str(tmp_path))
        key = get_cache_key(raw)
        assert (tmp_path / f"{key}.parquet").exists()
        assert len(df) == 5

    def test_cache_hit_returns_same_data(self, tmp_path):
        raw = _make_csv_bytes(5)
        df1 = load_or_cache(raw, cache_dir=str(tmp_path))
        df2 = load_or_cache(raw, cache_dir=str(tmp_path))
        assert len(df1) == len(df2)
        assert list(df1.columns) == list(df2.columns)

    def test_different_file_different_cache(self, tmp_path):
        raw1 = _make_csv_bytes(5)
        raw2 = _make_csv_bytes(8)
        load_or_cache(raw1, cache_dir=str(tmp_path))
        load_or_cache(raw2, cache_dir=str(tmp_path))
        parquets = list(tmp_path.glob("*.parquet"))
        assert len(parquets) == 2


class TestPrepareForIngest:
    def test_returns_tuple(self, tmp_path):
        raw = _make_csv_bytes(5)
        result = prepare_for_ingest(raw, cache_dir=str(tmp_path))
        assert isinstance(result, tuple)
        assert len(result) == 2
        df, key = result
        assert isinstance(df, pd.DataFrame)
        assert isinstance(key, str) and len(key) == 64

    def test_gbp_columns_present(self, tmp_path):
        raw = _make_csv_bytes(5)
        df, _ = prepare_for_ingest(raw, cache_dir=str(tmp_path))
        assert "break_amount_gbp" in df.columns

    def test_report_month_added(self, tmp_path):
        raw = _make_csv_bytes(5)
        df, _ = prepare_for_ingest(raw, cache_dir=str(tmp_path))
        assert "report_month" in df.columns
        assert df["report_month"].notna().any()
