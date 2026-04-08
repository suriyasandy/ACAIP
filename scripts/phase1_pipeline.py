"""
CASHFOARE PHASE 1 — FINAL END-TO-END PIPELINE
Lifecycle + Rec Flow + Journal + False Closure

Usage:
    python scripts/phase1_pipeline.py \\
        --cash  cashfoare_daily.csv \\
        --mi    monthly_mi.csv \\
        [--start 2025-12-01] \\
        [--end   2026-02-28] \\
        [--export]
"""

from __future__ import annotations

import argparse
import logging
import sys
from typing import Optional

import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# ─── Constants ────────────────────────────────────────────────────────────────

DEFAULT_START = pd.Timestamp("2025-12-01")
DEFAULT_END   = pd.Timestamp("2026-02-28")
MAX_AGE_DAYS  = 90
RECURRING_MIN_DAYS = 1          # Days_Active > this → Is_Recurring = True

# Rec-name → owning system.  Any rec not listed falls back to the text before
# the first separator (" vs ", " - ", space).
ROOT_SYSTEM_MAP: dict[str, str] = {
    "FOBO Rec-Equities":                                    "FOBO",
    "FOBO Rec-Dividend":                                    "FOBO",
    "Fidessa vs Fiscal vs FTP - Cash Equity":               "Fidessa",
    "Fidessa vs FTP - Stock/Index Futures":                 "Fidessa",
    "FLARE vs Fiscal Cash Control (Cash Equities)":         "FLARE",
    "FLARE vs HUB Cash Contra (EQD Contra/control)":        "FLARE",
    "FLARE vs HUB Cash Contra (L&D)":                       "FLARE",
    "Flare vs SYN - Cash Equities/SBL Control":             "FLARE",
    "Netted Cash Control - Cash equities/SBL":              "Netted",
    "CRI COMPTA Rec - Cash Equities/SBL":                   "COMPTA",
    "CRI COMPTA Rec - SED":                                 "COMPTA",
    "CRI COMPTA Rec - SEF":                                 "COMPTA",
    "Sophis vs FTP - Cash Equity (P&L - MTM & Cash)":       "Sophis",
    "Sophis vs FTP - EQD (P&L - MTM & Cash)":               "Sophis",
    "Sophis vs FTP - ETD PnL":                              "Sophis",
    "Sophis vs FTP - FX Spot/Fwd/NDF/FX swap PnL":          "Sophis",
    "P&L Rec - CEQ":                                        "P&L",
    "P&L Rec - SED":                                        "P&L",
    "P&L Rec - SEF":                                        "P&L",
    "Washbook '0006 Fiscal account' - Market Charges":      "Washbook",
}

# Rec precedence (lower = earlier in the settlement/reporting chain)
REC_ORDER: dict[str, int] = {
    "FOBO Rec-Equities":                                    1,
    "FOBO Rec-Dividend":                                    1,
    "Fidessa vs Fiscal vs FTP - Cash Equity":               2,
    "Fidessa vs FTP - Stock/Index Futures":                 2,
    "FLARE vs Fiscal Cash Control (Cash Equities)":         3,
    "FLARE vs HUB Cash Contra (EQD Contra/control)":        4,
    "FLARE vs HUB Cash Contra (L&D)":                       4,
    "Flare vs SYN - Cash Equities/SBL Control":             5,
    "Netted Cash Control - Cash equities/SBL":              6,
    "CRI COMPTA Rec - Cash Equities/SBL":                   7,
    "CRI COMPTA Rec - SED":                                 7,
    "CRI COMPTA Rec - SEF":                                 7,
    "Sophis vs FTP - Cash Equity (P&L - MTM & Cash)":       8,
    "Sophis vs FTP - EQD (P&L - MTM & Cash)":               8,
    "Sophis vs FTP - ETD PnL":                              8,
    "Sophis vs FTP - FX Spot/Fwd/NDF/FX swap PnL":          8,
    "P&L Rec - CEQ":                                        9,
    "P&L Rec - SED":                                        9,
    "P&L Rec - SEF":                                        9,
    "Washbook '0006 Fiscal account' - Market Charges":      10,
}

_REQUIRED_CASH_COLS = {"BREAK_DATE", "TRADE_ID", "PRODUCT", "BREAK"}
_REQUIRED_MI_COLS   = {"Date", "TRADE REF"}
_REC_NAME_COL       = "Rec Name (as per Rec Cube)"


# ─── Step 1: Standardise ──────────────────────────────────────────────────────

def standardize_cash(df: pd.DataFrame) -> pd.DataFrame:
    missing = _REQUIRED_CASH_COLS - set(df.columns)
    if missing:
        raise ValueError(f"cash_df is missing required columns: {missing}")

    df = df.copy()
    df["Date"]     = pd.to_datetime(df["BREAK_DATE"], errors="coerce")
    df["TRADE_ID"] = df["TRADE_ID"].astype(str).str.strip()
    df["PRODUCT"]  = df["PRODUCT"].astype(str).str.strip()
    df["BREAK"]    = pd.to_numeric(df["BREAK"], errors="coerce").fillna(0)
    return df


def standardize_mi(df: pd.DataFrame) -> pd.DataFrame:
    missing = _REQUIRED_MI_COLS - set(df.columns)
    if missing:
        raise ValueError(f"mi_df is missing required columns: {missing}")
    if _REC_NAME_COL not in df.columns:
        raise ValueError(
            f"mi_df must contain a '{_REC_NAME_COL}' column. "
            f"Columns found: {list(df.columns)}"
        )

    df = df.copy()
    df["Date"]      = pd.to_datetime(df["Date"], errors="coerce")
    df["TRADE REF"] = df["TRADE REF"].astype(str).str.strip()
    df["Rec"]       = df[_REC_NAME_COL].astype(str).str.strip()
    return df


# ─── Step 2: Date filter ──────────────────────────────────────────────────────

def filter_date_range(
    df: pd.DataFrame,
    date_col: str,
    start: pd.Timestamp,
    end: pd.Timestamp,
) -> pd.DataFrame:
    return df[(df[date_col] >= start) & (df[date_col] <= end)]


# ─── Step 3: Trade key + break chains ─────────────────────────────────────────

def build_break_chains(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["TradeKey"]    = df["PRODUCT"] + "|" + df["TRADE_ID"]
    df = df.sort_values(["TradeKey", "Date"])
    df["BreakChainID"] = df.groupby("TradeKey").ngroup()
    return df


# ─── Step 4: Chain summary ────────────────────────────────────────────────────

def build_chain_summary(df: pd.DataFrame) -> pd.DataFrame:
    summary = (
        df.groupby("BreakChainID")
        .agg(
            Trade        = ("TRADE_ID", "first"),
            Product      = ("PRODUCT",  "first"),
            Start_Date   = ("Date",     "min"),
            End_Date     = ("Date",     "max"),
            Days_Active  = ("Date",     "nunique"),
            Total_Breaks = ("BREAK",    "count"),
            Total_Amount = ("BREAK",    "sum"),
        )
        .reset_index()
    )
    summary["Is_Recurring"] = summary["Days_Active"] > RECURRING_MIN_DAYS
    return summary


# ─── Step 5: Rec flow ─────────────────────────────────────────────────────────

def ordered_unique(items: list) -> list:
    """Return items in first-seen order, duplicates removed.  O(n)."""
    return list(dict.fromkeys(items))


def build_rec_flow(mi_df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Returns (rec_flow_df, root_df).
    rec_flow_df : TRADE REF | Rec_List | Rec_Flow
    root_df     : TRADE REF | Root_Rec
    """
    mi = mi_df.copy()
    mi["rec_rank"] = mi["Rec"].map(REC_ORDER).fillna(999)
    mi = mi.sort_values(["TRADE REF", "Date", "rec_rank"])

    rec_flow_df = (
        mi.groupby("TRADE REF")
        .agg(
            Rec_List = ("Rec", lambda x: ordered_unique(list(x))),
            Rec_Flow = ("Rec", lambda x: " → ".join(ordered_unique(list(x)))),
        )
        .reset_index()
    )

    root_df = (
        mi.groupby("TRADE REF")
        .first()
        .reset_index()[["TRADE REF", "Rec"]]
        .rename(columns={"Rec": "Root_Rec"})
    )

    return rec_flow_df, root_df


# ─── Step 6: Journal flag ─────────────────────────────────────────────────────

def build_journal_flags(mi_df: pd.DataFrame) -> pd.DataFrame:
    if "Journals Posted" not in mi_df.columns:
        logger.warning(
            "'Journals Posted' column not found — Journal_Used will be False for all trades."
        )
        mi_df = mi_df.copy()
        mi_df["Journal_Flag"] = False
    else:
        mi_df = mi_df.copy()
        mi_df["Journal_Flag"] = mi_df["Journals Posted"].notna()

    return (
        mi_df.groupby("TRADE REF")
        .agg(Journal_Used=("Journal_Flag", "max"))
        .reset_index()
    )


# ─── Step 7: Merge ────────────────────────────────────────────────────────────

def build_final_summary(
    chain_summary: pd.DataFrame,
    rec_flow_df:   pd.DataFrame,
    root_df:       pd.DataFrame,
    journal_df:    pd.DataFrame,
) -> pd.DataFrame:
    df = chain_summary.copy()

    for right in (rec_flow_df, root_df, journal_df):
        df = df.merge(right, left_on="Trade", right_on="TRADE REF", how="left")
        df.drop(columns=["TRADE REF"], errors="ignore", inplace=True)

    # Ensure Journal_Used has no NaN before boolean logic
    df["Journal_Used"] = df["Journal_Used"].fillna(False)

    df["False_Closure"] = df["Journal_Used"] & df["Is_Recurring"]
    df["Root_System"]   = df["Root_Rec"].map(ROOT_SYSTEM_MAP).fillna(
        df["Root_Rec"].str.split(r" vs | - |\s", n=1).str[0]
    )
    return df


# ─── Step 8: KPIs ─────────────────────────────────────────────────────────────

def print_kpis(df: pd.DataFrame) -> None:
    total = len(df)
    if total == 0:
        logger.warning("No chains found for the selected date range — nothing to report.")
        return

    recurring     = int(df["Is_Recurring"].sum())
    journal       = int(df["Journal_Used"].sum())
    false_closure = int(df["False_Closure"].sum())

    logger.info("================ KPI SUMMARY ================")
    logger.info("Total Chains        : %d", total)
    logger.info("Recurring           : %d (%.1f%%)", recurring,     recurring     / total * 100)
    logger.info("Journal Used        : %d (%.1f%%)", journal,       journal       / total * 100)
    logger.info("False Closure       : %d (%.1f%%)", false_closure, false_closure / total * 100)


# ─── Step 9: Root system summary ─────────────────────────────────────────────

def print_root_system_summary(df: pd.DataFrame) -> None:
    summary = (
        df.groupby("Root_System")
        .agg(
            Total_Chains   = ("BreakChainID", "count"),
            False_Closures = ("False_Closure", "sum"),
        )
        .reset_index()
        .sort_values("Total_Chains", ascending=False)
    )
    logger.info("===== TOP ROOT SYSTEMS =====\n%s", summary.head(10).to_string(index=False))


# ─── Step 10: Top false closures ─────────────────────────────────────────────

def print_top_false_closures(df: pd.DataFrame) -> None:
    top = (
        df[df["False_Closure"]]
        .sort_values("Days_Active", ascending=False)
        [["Trade", "Product", "Days_Active", "Rec_Flow", "Root_Rec"]]
        .head(10)
    )
    logger.info("===== TOP FALSE CLOSURE TRADES =====\n%s", top.to_string(index=False))


# ─── CLI entry point ──────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="CASHFOARE Phase 1 Pipeline")
    parser.add_argument("--cash",   required=True,  help="Path to cashfoare daily CSV")
    parser.add_argument("--mi",     required=True,  help="Path to monthly MI report CSV")
    parser.add_argument("--start",  default="2025-12-01", help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end",    default="2026-02-28", help="End date (YYYY-MM-DD)")
    parser.add_argument("--export", action="store_true",  help="Export final_summary to CSV")
    return parser.parse_args()


def main(
    cash_path: str,
    mi_path:   str,
    start:     Optional[str] = None,
    end:       Optional[str] = None,
    export:    bool = False,
) -> pd.DataFrame:
    start_ts = pd.Timestamp(start or DEFAULT_START)
    end_ts   = pd.Timestamp(end   or DEFAULT_END)

    # ── Load ──────────────────────────────────────────────────────────────────
    logger.info("Loading cash data from %s", cash_path)
    raw_cash = pd.read_csv(cash_path)

    logger.info("Loading MI data from %s", mi_path)
    raw_mi = pd.read_csv(mi_path)

    # ── Standardise ───────────────────────────────────────────────────────────
    cash_df = standardize_cash(raw_cash)
    mi_df   = standardize_mi(raw_mi)

    # ── Filter ────────────────────────────────────────────────────────────────
    cash_df = filter_date_range(cash_df, "Date", start_ts, end_ts)
    mi_df   = filter_date_range(mi_df,   "Date", start_ts, end_ts)

    if "Age Days" in mi_df.columns:
        mi_df = mi_df[mi_df["Age Days"] <= MAX_AGE_DAYS]

    # ── Build components ──────────────────────────────────────────────────────
    cash_df      = build_break_chains(cash_df)
    chain_summary = build_chain_summary(cash_df)
    rec_flow_df, root_df = build_rec_flow(mi_df)
    journal_df   = build_journal_flags(mi_df)

    # ── Merge & enrich ────────────────────────────────────────────────────────
    final_summary = build_final_summary(chain_summary, rec_flow_df, root_df, journal_df)

    # ── Report ────────────────────────────────────────────────────────────────
    print_kpis(final_summary)
    print_root_system_summary(final_summary)
    print_top_false_closures(final_summary)

    # ── Export ────────────────────────────────────────────────────────────────
    if export:
        out = "final_break_summary.csv"
        final_summary.to_csv(out, index=False)
        logger.info("Exported to %s", out)

    logger.info("[DONE] Phase 1 Completed Successfully")
    return final_summary


if __name__ == "__main__":
    args = parse_args()
    main(
        cash_path=args.cash,
        mi_path=args.mi,
        start=args.start,
        end=args.end,
        export=args.export,
    )
