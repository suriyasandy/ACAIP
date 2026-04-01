"""Seed the DuckDB break_ledger with realistic synthetic data for dashboard development."""
import sys
import os
import random
from datetime import date, timedelta

import pandas as pd
import numpy as np

# Allow running as `python backend/seed_data.py` from repo root
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from backend.database.duckdb_manager import (
    init_db, upsert_breaks, upsert_jira, upsert_rec_configs, load_fx_rates, get_conn,
)

random.seed(42)
np.random.seed(42)

# ── Configuration ──────────────────────────────────────────────────────────────
N_BREAKS = 2000
TODAY = date.today()

PLATFORMS = ["TLM", "DUCO", "D3S"]
ASSET_CLASSES = ["CEQ", "LnD", "MTN", "OTC"]

RECS = [
    ("TLM-001", "TLM Equity Settlement",   "TLM",  "CEQ"),
    ("TLM-002", "TLM Fixed Income",        "TLM",  "MTN"),
    ("TLM-003", "TLM FX Nostro",           "TLM",  "LnD"),
    ("TLM-004", "TLM Derivatives Margin",  "TLM",  "OTC"),
    ("TLM-005", "TLM Corp Actions",        "TLM",  "CEQ"),
    ("DUCO-01", "DUCO Prime Brokerage",    "DUCO", "CEQ"),
    ("DUCO-02", "DUCO Repo / SBL",         "DUCO", "LnD"),
    ("DUCO-03", "DUCO OTC Margin",         "DUCO", "OTC"),
    ("D3S-CEQ", "D3S Cash Equities",       "D3S",  "CEQ"),
    ("D3S-LND", "D3S Loans & Deposits",    "D3S",  "LnD"),
    ("D3S-MTN", "D3S Medium Term Notes",   "D3S",  "MTN"),
    ("D3S-OTC", "D3S OTC Derivatives",     "D3S",  "OTC"),
]

THEMES = [
    "Price Mismatch",
    "Settlement Timing",
    "Accrual Difference",
    "Coupon Dispute",
    "NPV Valuation Gap",
    "Corporate Action",
    "FX Rate Discrepancy",
    "Margin Call",
    "Trade Reference Error",
    "System Outage",
]

STATUSES = ["OPEN", "OPEN", "OPEN", "CLOSED", "RESOLVED"]

ISSUE_CATS = [
    "Pricing", "Settlement", "Accrual", "Coupon", "Valuation",
    "Corp Action", "FX", "Margin", "Reference Data", "System",
]

ISSUE_CATS_2 = [
    "Front Office", "Back Office", "Middle Office", "Counterparty", "CSD", "Vendor",
]

SLA_MAP = {"CEQ": 3, "LnD": 5, "MTN": 7, "OTC": 1}

CCYS = ["GBP", "USD", "EUR", "JPY", "CHF"]

FX_RATES_TO_GBP = {"GBP": 1.0, "USD": 0.79, "EUR": 0.85, "JPY": 0.0053, "CHF": 0.89}

AGE_BUCKETS = {
    range(0, 4): "0-3d",
    range(4, 8): "3-7d",
    range(8, 31): "7-30d",
}


def age_bucket(age):
    for r, label in AGE_BUCKETS.items():
        if age in r:
            return label
    return "30d+"


def make_trade_ref():
    return f"TR-{random.randint(100000, 999999)}"


def make_jira_ref():
    epics = ["EQ-BREAKS", "LND-BREAKS", "MTN-BREAKS", "OTC-BREAKS"]
    epic = random.choice(epics)
    return f"{epic}-{random.randint(1000, 9999)}"


# ── Rec configs ────────────────────────────────────────────────────────────────
def build_rec_configs() -> pd.DataFrame:
    rows = []
    for rec_id, rec_name, platform, ac in RECS:
        rows.append({
            "rec_id": rec_id,
            "rec_name": rec_name,
            "source_platform": platform,
            "asset_class": ac,
            "d3s_asset_class": ac if platform == "D3S" else None,
            "threshold_type": "PCT",
            "threshold_pct": {"CEQ": 0.5, "LnD": 0.1, "MTN": 0.2, "OTC": 0.05}[ac],
            "threshold_abs_gbp": {"CEQ": 300_000, "LnD": 500_000, "MTN": 750_000, "OTC": 250_000}[ac],
            "escalation_sla_days": SLA_MAP[ac],
            "jira_epic": f"{ac[:3] if ac != 'LnD' else 'LND'}-BREAKS",
            "emir_flag": ac == "OTC",
            "self_correct_days": 2,
            "ml_model_id": f"rf_{ac.lower()}_v1",
            "active": True,
        })
    return pd.DataFrame(rows)


# ── Breaks ─────────────────────────────────────────────────────────────────────
def build_breaks() -> pd.DataFrame:
    rows = []
    for i in range(N_BREAKS):
        rec_id, rec_name, platform, ac = random.choice(RECS)
        ccy = random.choice(CCYS)
        break_value = round(random.uniform(-2_000_000, 2_000_000), 2)
        fx = FX_RATES_TO_GBP[ccy]
        abs_gbp = round(abs(break_value) * fx, 2)
        age = random.randint(0, 60)
        report_date = TODAY - timedelta(days=random.randint(0, 29))
        first_seen = report_date - timedelta(days=age)
        sla = SLA_MAP[ac]
        days_to_sla = sla - age
        sla_breach = days_to_sla < 0
        material = abs_gbp > {"CEQ": 300_000, "LnD": 500_000, "MTN": 750_000, "OTC": 250_000}[ac]
        status = random.choice(STATUSES)
        has_jira = random.random() < (0.95 if material else 0.45)
        emir = ac == "OTC"
        escalation = ""
        if age > sla:
            escalation = random.choice(["MANAGER", "HEAD", "REGULATORY"])

        rows.append({
            "source_system": platform,
            "asset_class": ac,
            "rec_name": rec_name,
            "rec_id": rec_id,
            "trade_ref": make_trade_ref(),
            "break_type": random.choice(["PRICE", "QTY", "SETTLEMENT", "ACCRUAL", "VALUATION"]),
            "break_value": break_value,
            "break_ccy": ccy,
            "abs_gbp": abs_gbp,
            "fx_rate": fx,
            "age_days": age,
            "age_bucket": age_bucket(age),
            "day_of_month": report_date.day,
            "period": report_date.strftime("%Y-%m"),
            "report_date": report_date,
            "jira_ref": make_jira_ref() if has_jira else None,
            "jira_desc": f"Auto-detected break on {rec_name}" if has_jira else None,
            "issue_category": random.choice(ISSUE_CATS),
            "issue_category_2": random.choice(ISSUE_CATS_2),
            "jira_priority": random.choice(["P1", "P2", "P3"]) if has_jira else None,
            "epic": f"{ac[:3] if ac != 'LnD' else 'LND'}-BREAKS",
            "system_to_be_fixed": random.choice(["Front Office", "Back Office", "Vendor"]),
            "fix_required": random.random() < 0.6,
            "ml_risk_score": round(random.uniform(0, 1), 4),
            "thematic": random.choice(THEMES) if random.random() < 0.8 else None,
            "type_of_issue": random.choice(["STATIC", "DYNAMIC", "HISTORIC"]),
            "recurring_break_flag": random.random() < 0.3,
            "cross_platform_match": random.random() < 0.15,
            "historical_match_confidence": round(random.uniform(0.5, 1.0), 3) if has_jira else None,
            "action": random.choice(["PENDING", "IN_PROGRESS", "AWAITING_CP", None]),
            "bs_cert_ready": random.random() < 0.5,
            "threshold_breach": material,
            "material_flag": material,
            "escalation_flag": escalation,
            "sla_breach": sla_breach,
            "days_to_sla": days_to_sla,
            "emir_flag": emir,
            "d3s_asset_class": ac if platform == "D3S" else None,
            "first_seen_date": first_seen,
            "last_seen_date": report_date,
            "status": status,
        })
    return pd.DataFrame(rows)


# ── Jira tickets ────────────────────────────────────────────────────────────────
def build_jira(breaks_df: pd.DataFrame) -> pd.DataFrame:
    has_jira = breaks_df[breaks_df["jira_ref"].notna()].copy()
    seen = set()
    rows = []
    for _, b in has_jira.iterrows():
        ref = b["jira_ref"]
        if ref in seen:
            continue
        seen.add(ref)
        is_draft = random.random() < 0.15
        status = "DRAFT" if is_draft else random.choice(["OPEN", "IN_PROGRESS", "CLOSED", "RESOLVED"])
        rows.append({
            "jira_ref": ref,
            "trade_ref": b["trade_ref"],
            "source_system": b["source_system"],
            "rec_id": b["rec_id"],
            "epic": b["epic"],
            "status": status,
            "summary": f"Break on {b['rec_name']}: {b['break_type']} {b['break_ccy']}",
            "assignee_team": random.choice(["Equity Ops", "FI Ops", "Derivatives Ops", "Repo Desk"]),
            "created_date": b["first_seen_date"],
            "resolved_date": b["last_seen_date"] if status in ("CLOSED", "RESOLVED") else None,
            "tags": f"{b['thematic']},{b['asset_class']}" if b["thematic"] else b["asset_class"],
            "break_value_gbp": b["abs_gbp"],
            "asset_class": b["asset_class"],
            "is_draft": is_draft,
        })
    return pd.DataFrame(rows)


# ── FX rates ────────────────────────────────────────────────────────────────────
def build_fx() -> pd.DataFrame:
    rows = []
    pairs = [(ccy, "GBP") for ccy in CCYS if ccy != "GBP"]
    for ccy, base in pairs:
        pair = f"{ccy}/{base}"
        for d in range(60):
            dt = TODAY - timedelta(days=d)
            rate = FX_RATES_TO_GBP[ccy] * (1 + random.uniform(-0.01, 0.01))
            rows.append({"ccy_pair": pair, "rate_date": dt, "rate": round(rate, 6)})
    return pd.DataFrame(rows)


# ── Main ──────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("Initialising database schema...")
    init_db()

    print("Seeding rec_configs...")
    rc = build_rec_configs()
    upsert_rec_configs(rc)
    print(f"  Loaded {len(rc)} rec configs")

    print("Seeding breaks...")
    bdf = build_breaks()
    upsert_breaks(bdf)
    print(f"  Loaded {len(bdf)} breaks")

    print("Seeding jira_tickets...")
    jdf = build_jira(bdf)
    upsert_jira(jdf)
    print(f"  Loaded {len(jdf)} jira tickets")

    print("Seeding fx_rates...")
    fdf = build_fx()
    load_fx_rates(fdf)
    print(f"  Loaded {len(fdf)} FX rates")

    conn = get_conn()
    print("\nRow counts:")
    for table in ["breaks", "jira_tickets", "rec_configs", "fx_rates"]:
        n = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        print(f"  {table}: {n}")

    print("\nDone – database seeded successfully.")
