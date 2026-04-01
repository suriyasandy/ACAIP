import pandas as pd
from backend.database.duckdb_manager import query_df, get_conn


def match(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    jira_df = query_df("""
        SELECT trade_ref, source_system, jira_ref, status AS jira_status,
               summary AS jira_desc, epic, asset_class,
               break_value_gbp, assignee_team
        FROM jira_tickets
    """)
    if jira_df.empty:
        df["historical_match_confidence"] = 0.0
        df["cross_platform_match"] = False
        df["recurring_break_flag"] = False
        return df
    exact = jira_df.rename(columns={
        "jira_ref": "_jira_ref", "jira_desc": "_jira_desc",
        "epic": "_epic", "jira_status": "_jira_status"
    })
    merged = df.merge(
        exact[["trade_ref", "source_system", "_jira_ref", "_jira_desc", "_epic", "_jira_status"]],
        on=["trade_ref", "source_system"], how="left"
    )
    mask_exact = merged["_jira_ref"].notna()
    df.loc[mask_exact, "jira_ref"]  = merged.loc[mask_exact, "_jira_ref"].values
    df.loc[mask_exact, "jira_desc"] = merged.loc[mask_exact, "_jira_desc"].values
    df.loc[mask_exact, "epic"]      = merged.loc[mask_exact, "_epic"].values
    df.loc[mask_exact, "historical_match_confidence"] = 1.0
    df.loc[mask_exact, "recurring_break_flag"] = merged.loc[mask_exact, "_jira_status"].isin(["CLOSED", "RESOLVED"]).values
    cross = jira_df.drop_duplicates("trade_ref")
    cross_merged = df[["trade_ref", "source_system"]].merge(
        cross[["trade_ref", "source_system"]].rename(columns={"source_system": "other_system"}),
        on="trade_ref", how="left"
    )
    cross_flag = (
        cross_merged["other_system"].notna() &
        (cross_merged["other_system"] != cross_merged["source_system"])
    )
    df["cross_platform_match"] = cross_flag.values
    df["historical_match_confidence"] = pd.to_numeric(
        df.get("historical_match_confidence"), errors="coerce"
    ).fillna(0.0)
    df["recurring_break_flag"] = df.get("recurring_break_flag", False).fillna(False)
    return df


def create_draft_tickets(df: pd.DataFrame) -> pd.DataFrame:
    import uuid
    from backend.utils.constants import ASSET_CLASS_RULES
    no_jira = df[df["jira_ref"].isna()].copy()
    if no_jira.empty:
        return pd.DataFrame()
    rows = []
    for _, row in no_jira.iterrows():
        jira_ref = f"DRAFT-{str(uuid.uuid4())[:8].upper()}"
        ac = row.get("asset_class", "Cash Equities") or "Cash Equities"
        rules = ASSET_CLASS_RULES.get(ac, ASSET_CLASS_RULES["Cash Equities"])
        rows.append({
            "jira_ref": jira_ref,
            "trade_ref": row.get("trade_ref"),
            "source_system": row.get("source_system"),
            "rec_id": row.get("rec_id"),
            "epic": row.get("epic") or rules["jira_epic"],
            "status": "DRAFT",
            "summary": f"Auto-draft: {row.get('break_type','Break')} | {row.get('asset_class','')} | {row.get('rec_name','')}",
            "assignee_team": None,
            "created_date": str(row.get("report_date") or pd.Timestamp.today().date()),
            "resolved_date": None,
            "tags": None,
            "break_value_gbp": row.get("abs_gbp"),
            "asset_class": row.get("asset_class"),
            "is_draft": True,
        })
    return pd.DataFrame(rows)
