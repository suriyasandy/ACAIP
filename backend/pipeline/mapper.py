"""Column mapping utilities: auto-suggest and apply user-defined field mappings."""
import re
import pandas as pd


# ── Columns that are auto-calculated and should NOT appear in the mapping dropdown
AUTO_CALCULATED = {
    "id", "load_ts",
    "fx_rate",
    "day_of_month", "period",
    "material_flag", "threshold_breach",
    "sla_breach", "days_to_sla",
    "emir_flag",
    "ml_risk_score", "ml_rag_prediction",
}

# ── Known aliases: normalized file column name → schema column name
# Covers the real D3S Monthly MI column names and common variations
KNOWN_ALIASES = {
    # Trade identifiers
    "traderef":                  "trade_ref",
    "tradereference":            "trade_ref",
    # Currency / break value
    "tradeccy":                  "break_ccy",
    "breakamountccy":            "break_value",
    "breakamount":               "break_value",
    "breakamountgbp":            "abs_gbp",
    "absgbp":                    "abs_gbp",
    # Dates / age
    "date":                      "report_date",
    "agedays":                   "age_days",
    "bucket":                    "age_bucket",
    # Rec identifiers
    "recnameasperrecube":        "rec_name",
    "recnameasperrecccube":      "rec_name",
    "recscubename":              "rec_id",
    "recscube":                  "rec_id",
    # Break classification
    "typeofbreak":               "break_type",
    "assetclass":                "asset_class",
    # Jira
    "jirareference":             "jira_ref",
    "jiradesc":                  "jira_desc",
    "jiradescdesc":              "jira_desc",
    "comments":                  "jira_desc",
    "issuecategory":             "issue_category",
    "issuecategory2":            "issue_category_2",
    "jirapriority":              "jira_priority",
    "epicdesc":                  "epic_desc",
    # Workflow
    "systemtobefixed":           "system_to_be_fixed",
    "fixrequired":               "fix_required",
    "bscert":                    "bs_cert_ready",
    # D3S / MI extended fields
    "issueragrating":            "rag_rating",
    "thematic":                  "thematic",
    "typeofissue":               "type_of_issue",
    "rootcauseidentified":       "root_cause_identified",
    "truesystemicbreaks":        "true_systemic",
    "journalsposted":            "journals_posted",
    "cashnoncash":               "cash_non_cash",
    "highlevelproduct":          "high_level_product",
    "productsreconciled":        "high_level_product",
    "entity":                    "entity",
    "team":                      "team",
    "accountgroup":              "account_group",
    # Misc
    "action":                    "action",
    "period":                    "period",
    "month":                     "period",
}

# ── All mappable breaks columns with human-readable labels and categories
SCHEMA_COLUMNS = [
    # Identifiers
    {"name": "trade_ref",           "label": "Trade Reference",            "required": True,  "category": "identifier"},
    {"name": "rec_id",              "label": "Rec ID (RECS_CUBE_NAME)",    "required": False, "category": "identifier"},
    {"name": "rec_name",            "label": "Rec Name",                   "required": False, "category": "identifier"},
    {"name": "source_system",       "label": "Source System / Platform",   "required": False, "category": "identifier"},
    {"name": "asset_class",         "label": "Asset Class",                "required": False, "category": "identifier"},
    {"name": "entity",              "label": "Entity",                     "required": False, "category": "identifier"},
    {"name": "team",                "label": "Team",                       "required": False, "category": "identifier"},
    {"name": "account_group",       "label": "Account Group",              "required": False, "category": "identifier"},
    # Break details
    {"name": "break_value",         "label": "Break Amount (CCY)",         "required": True,  "category": "financials"},
    {"name": "break_ccy",           "label": "Trade CCY",                  "required": False, "category": "financials"},
    {"name": "abs_gbp",             "label": "Break Amount GBP / ABS GBP","required": False, "category": "financials"},
    {"name": "break_type",          "label": "Type of Break",              "required": False, "category": "financials"},
    {"name": "high_level_product",  "label": "High Level Product",         "required": False, "category": "financials"},
    {"name": "cash_non_cash",       "label": "Cash / Non-Cash",            "required": False, "category": "financials"},
    # Dates
    {"name": "report_date",         "label": "Date / Report Date",         "required": False, "category": "dates"},
    {"name": "first_seen_date",     "label": "First Seen Date",            "required": False, "category": "dates"},
    {"name": "last_seen_date",      "label": "Last Seen Date",             "required": False, "category": "dates"},
    {"name": "age_days",            "label": "Age Days",                   "required": False, "category": "dates"},
    {"name": "age_bucket",          "label": "Bucket (Age Band)",          "required": False, "category": "dates"},
    {"name": "period",              "label": "Period / Month",             "required": False, "category": "dates"},
    # Jira / classification
    {"name": "jira_ref",            "label": "Jira Reference",             "required": False, "category": "jira"},
    {"name": "jira_desc",           "label": "Jira Description / Comments","required": False, "category": "jira"},
    {"name": "issue_category",      "label": "Issue Category 1",           "required": False, "category": "jira"},
    {"name": "issue_category_2",    "label": "Issue Category 2",           "required": False, "category": "jira"},
    {"name": "jira_priority",       "label": "Jira Priority",              "required": False, "category": "jira"},
    {"name": "epic",                "label": "Jira Epic",                  "required": False, "category": "jira"},
    {"name": "epic_desc",           "label": "Epic Description",           "required": False, "category": "jira"},
    # Workflow
    {"name": "status",              "label": "Status",                     "required": False, "category": "workflow"},
    {"name": "action",              "label": "Action",                     "required": False, "category": "workflow"},
    {"name": "system_to_be_fixed",  "label": "System to be Fixed",        "required": False, "category": "workflow"},
    {"name": "fix_required",        "label": "Fix Required",               "required": False, "category": "workflow"},
    {"name": "bs_cert_ready",       "label": "B/S Cert Ready",             "required": False, "category": "workflow"},
    {"name": "journals_posted",     "label": "Journals Posted",            "required": False, "category": "workflow"},
    # Analysis / MI extended
    {"name": "thematic",            "label": "Thematic / Root Cause",      "required": False, "category": "analysis"},
    {"name": "true_systemic",       "label": "True / Systemic Breaks",     "required": False, "category": "analysis"},
    {"name": "rag_rating",          "label": "Issue RAG Rating",           "required": False, "category": "analysis"},
    {"name": "type_of_issue",       "label": "Type of Issue",              "required": False, "category": "analysis"},
    {"name": "root_cause_identified","label": "Root Cause Identified",     "required": False, "category": "analysis"},
    {"name": "d3s_asset_class",     "label": "D3S Asset Class",            "required": False, "category": "analysis"},
    {"name": "historical_match_confidence", "label": "Historical Match Confidence", "required": False, "category": "analysis"},
]

_SCHEMA_NAMES = [c["name"] for c in SCHEMA_COLUMNS]


def _normalize(s: str) -> str:
    """Lowercase and strip all non-alphanumeric characters for fuzzy matching."""
    return re.sub(r"[^a-z0-9]", "", s.lower())


def auto_suggest(headers: list[str]) -> dict[str, str | None]:
    """Return {file_column: schema_column} auto-suggestions for a list of file headers.

    Uses a tiered match strategy:
      0. KNOWN_ALIASES exact lookup (handles real MI column names)
      1. Exact normalized match against schema column names
      2. Schema col name is contained within the normalized file header
      3. Normalized file header is contained within the schema col name
    Unmatched columns map to None. One-to-one: each schema col claimed once.
    """
    norm_schema = {_normalize(c): c for c in _SCHEMA_NAMES}
    claimed: set[str] = set()
    suggestions: dict[str, str | None] = {}

    for hdr in headers:
        norm_hdr = _normalize(hdr)
        match = None

        # Tier 0: known alias lookup
        alias = KNOWN_ALIASES.get(norm_hdr)
        if alias and alias not in claimed:
            match = alias

        # Tier 1: exact normalized match
        if match is None and norm_hdr in norm_schema:
            candidate = norm_schema[norm_hdr]
            if candidate not in claimed:
                match = candidate

        # Tier 2: schema name contained in header
        if match is None:
            for ns, sc in norm_schema.items():
                if ns and ns in norm_hdr and sc not in claimed:
                    match = sc
                    break

        # Tier 3: header contained in schema name
        if match is None:
            for ns, sc in norm_schema.items():
                if norm_hdr and norm_hdr in ns and sc not in claimed:
                    match = sc
                    break

        if match:
            claimed.add(match)
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
