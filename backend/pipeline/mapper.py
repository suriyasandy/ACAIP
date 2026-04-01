"""Column mapping utilities: auto-suggest and apply user-defined field mappings."""
import re
import pandas as pd


# ── Columns that are auto-calculated and should NOT appear in the mapping dropdown
AUTO_CALCULATED = {
    "id", "load_ts",
    "abs_gbp", "fx_rate",
    "age_bucket", "day_of_month", "period",
    "material_flag", "threshold_breach",
    "sla_breach", "days_to_sla",
    "emir_flag",
    "ml_risk_score",
}

# ── All mappable breaks columns with human-readable labels and categories
SCHEMA_COLUMNS = [
    # Identifiers
    {"name": "trade_ref",           "label": "Trade Reference",       "required": True,  "category": "identifier"},
    {"name": "rec_id",              "label": "Rec ID",                "required": False, "category": "identifier"},
    {"name": "rec_name",            "label": "Rec Name",              "required": False, "category": "identifier"},
    {"name": "source_system",       "label": "Source System / Platform","required": False, "category": "identifier"},
    {"name": "asset_class",         "label": "Asset Class",           "required": False, "category": "identifier"},
    # Break details
    {"name": "break_value",         "label": "Break Value",           "required": True,  "category": "financials"},
    {"name": "break_ccy",           "label": "Break Currency",        "required": False, "category": "financials"},
    {"name": "break_type",          "label": "Break Type",            "required": False, "category": "financials"},
    # Dates
    {"name": "report_date",         "label": "Report Date",           "required": False, "category": "dates"},
    {"name": "first_seen_date",     "label": "First Seen Date",       "required": False, "category": "dates"},
    {"name": "last_seen_date",      "label": "Last Seen Date",        "required": False, "category": "dates"},
    {"name": "age_days",            "label": "Age (Days)",            "required": False, "category": "dates"},
    # Jira / classification
    {"name": "jira_ref",            "label": "Jira Reference",        "required": False, "category": "jira"},
    {"name": "jira_desc",           "label": "Jira Description",      "required": False, "category": "jira"},
    {"name": "issue_category",      "label": "Issue Category 1",      "required": False, "category": "jira"},
    {"name": "issue_category_2",    "label": "Issue Category 2",      "required": False, "category": "jira"},
    {"name": "jira_priority",       "label": "Jira Priority",         "required": False, "category": "jira"},
    {"name": "epic",                "label": "Jira Epic",             "required": False, "category": "jira"},
    # Workflow
    {"name": "status",              "label": "Status",                "required": False, "category": "workflow"},
    {"name": "action",              "label": "Action",                "required": False, "category": "workflow"},
    {"name": "system_to_be_fixed",  "label": "System to be Fixed",   "required": False, "category": "workflow"},
    {"name": "fix_required",        "label": "Fix Required",          "required": False, "category": "workflow"},
    # Metadata
    {"name": "thematic",            "label": "Thematic / Root Cause", "required": False, "category": "metadata"},
    {"name": "type_of_issue",       "label": "Type of Issue",         "required": False, "category": "metadata"},
    {"name": "d3s_asset_class",     "label": "D3S Asset Class",       "required": False, "category": "metadata"},
    {"name": "historical_match_confidence", "label": "Historical Match Confidence", "required": False, "category": "metadata"},
]

_SCHEMA_NAMES = [c["name"] for c in SCHEMA_COLUMNS]


def _normalize(s: str) -> str:
    """Lowercase and strip all non-alphanumeric characters for fuzzy matching."""
    return re.sub(r"[^a-z0-9]", "", s.lower())


def auto_suggest(headers: list[str]) -> dict[str, str | None]:
    """Return {file_column: schema_column} auto-suggestions for a list of file headers.

    Uses a tiered match strategy:
      1. Exact normalized match
      2. Schema col name is contained within the normalized file header
      3. Normalized file header is contained within the schema col name
    Unmatched columns map to None.
    """
    norm_schema = {_normalize(c): c for c in _SCHEMA_NAMES}
    suggestions = {}
    for hdr in headers:
        norm_hdr = _normalize(hdr)
        match = None
        # Tier 1: exact
        if norm_hdr in norm_schema:
            match = norm_schema[norm_hdr]
        if match is None:
            # Tier 2: schema name contained in header (e.g. "traderef" in "tradereferencenum")
            for ns, sc in norm_schema.items():
                if ns and ns in norm_hdr:
                    match = sc
                    break
        if match is None:
            # Tier 3: header contained in schema name
            for ns, sc in norm_schema.items():
                if norm_hdr and norm_hdr in ns:
                    match = sc
                    break
        suggestions[hdr] = match
    return suggestions


def apply_mapping(df: pd.DataFrame, column_mapping: dict[str, str]) -> pd.DataFrame:
    """Rename file columns according to mapping dict, dropping unmapped/ignored columns.

    Args:
        df: Raw file DataFrame with original column names.
        column_mapping: {file_col: schema_col} — values of None / "" / "__ignore__" are dropped.

    Returns:
        DataFrame with only mapped columns, renamed to schema names.
    """
    rename = {k: v for k, v in column_mapping.items()
              if v and v not in ("", "__ignore__", None)}
    # Only rename columns that exist in the df
    rename = {k: v for k, v in rename.items() if k in df.columns}
    result = df[list(rename.keys())].rename(columns=rename)
    # Deduplicate: if two file cols mapped to the same schema col, keep the first
    result = result.loc[:, ~result.columns.duplicated(keep="first")]
    return result
