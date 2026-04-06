"""
CASHFOARE ML False Break Detection — Streamlit Application
==========================================================
6-page multi-layout app:
  1. Break Overview
  2. ML Scoring
  3. Drill-Down Analysis
  4. Data Quality
  5. Feedback & Retraining
  6. Export
"""

from __future__ import annotations

import io
from datetime import datetime
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode, JsCode

from modules.data_loader import PENCE_COLUMNS, log_upload, prepare_for_ingest
from modules.db import (
    get_connection,
    get_kpis,
    ingest_dataframe,
    init_schema,
    query_breaks,
    update_risk_tiers,
)
from modules.dq_scorer import compute_and_persist_dq, compute_dq_scores, get_dq_summary
from modules.false_break_model import load_model, train_from_dataframe
from modules.feedback_store import (
    get_feedback_count,
    get_feedback_for_retraining,
    load_feedback,
    save_feedback,
)
from modules.ml_features import ALL_FEATURES, build_feature_matrix, get_feature_names
from modules.scorer import compute_suppression_stats, score_and_persist, score_breaks
from modules.shap_explainer import explain_break, get_top_features
from modules.weak_labeller import label_breaks

# ─────────────────────────────── constants ───────────────────────────────────

VALID_ASSET_CLASSES = ["CEQ", "LnD", "MTN", "OTC"]
DB_PATH = str(Path("data") / "cashfoare.duckdb")
MODELS_DIR = "models"

# AgGrid CSS injected once at startup
AGGRID_CSS = """
<style>
.row-suppress td { background-color: #ffd6d6 !important; }
.row-review   td { background-color: #fff3cd !important; }
.row-escalate td { background-color: #d4edda !important; }
.row-high-priority td { font-weight: bold !important; border-left: 4px solid #dc3545 !important; }
.stMetric > div { background: #f8f9fa; border-radius: 8px; padding: 8px; }
</style>
"""

# JS cellRenderer for risk_tier chip
RISK_TIER_RENDERER = JsCode("""
function(params) {
    const colours = {Suppress:'#dc3545', Review:'#fd7e14', Escalate:'#28a745', Unscored:'#6c757d'};
    const tier = params.value || 'Unscored';
    const bg = colours[tier] || '#6c757d';
    return `<span style="background:${bg};color:white;padding:2px 8px;border-radius:12px;font-size:12px">${tier}</span>`;
}
""")

# JS rowClassRules for row-level colouring
ROW_CLASS_RULES = {
    "row-suppress":     "data.risk_tier === 'Suppress'",
    "row-review":       "data.risk_tier === 'Review'",
    "row-escalate":     "data.risk_tier === 'Escalate'",
    "row-high-priority": "(data.governance_completeness_score < 0.5) || (data.days_overdue_vs_sla > 30)",
}

# JS cell style for dq_score gradient
DQ_CELL_STYLE = JsCode("""
function(params) {
    if (params.value == null) return {};
    const v = parseFloat(params.value);
    if (v < 0.4) return {backgroundColor:'#f8d7da', color:'#721c24'};
    if (v < 0.7) return {backgroundColor:'#fff3cd', color:'#856404'};
    return {backgroundColor:'#d4edda', color:'#155724'};
}
""")

PROB_CELL_STYLE = JsCode("""
function(params) {
    if (params.value == null) return {};
    const v = parseFloat(params.value);
    if (v > 0.80) return {backgroundColor:'#ffd6d6', fontWeight:'bold'};
    if (v > 0.50) return {backgroundColor:'#fff3cd'};
    return {backgroundColor:'#d4edda'};
}
""")


# ─────────────────────────────── app init ────────────────────────────────────

st.set_page_config(
    page_title="CASHFOARE ML | False Break Detection",
    page_icon="🔍",
    layout="wide",
    initial_sidebar_state="expanded",
)
st.markdown(AGGRID_CSS, unsafe_allow_html=True)

# Initialise session state
if "db_conn" not in st.session_state:
    conn = get_connection(DB_PATH)
    init_schema(conn)
    st.session_state.db_conn = conn

if "fp_threshold" not in st.session_state:
    st.session_state.fp_threshold = 0.80
if "asset_filter" not in st.session_state:
    st.session_state.asset_filter = []
if "selected_break_id" not in st.session_state:
    st.session_state.selected_break_id = None
if "model_version" not in st.session_state:
    st.session_state.model_version = "No model trained yet"
if "asset_thresholds" not in st.session_state:
    st.session_state.asset_thresholds = {}
if "debug_mode" not in st.session_state:
    st.session_state.debug_mode = False

conn = st.session_state.db_conn


# ─────────────────────────────── sidebar ─────────────────────────────────────

with st.sidebar:
    st.image("https://via.placeholder.com/200x50?text=CASHFOARE+ML", use_container_width=True)
    st.title("Navigation")
    page = st.radio(
        "Page",
        ["Break Overview", "ML Scoring", "Drill-Down", "Data Quality",
         "Feedback & Retraining", "Export"],
        label_visibility="collapsed",
    )
    st.divider()
    st.caption(f"Model: {st.session_state.model_version}")
    st.caption(f"Threshold: {st.session_state.fp_threshold:.2f}")
    st.session_state.debug_mode = st.toggle("Debug mode (show pseudo_label)", False)
    st.divider()
    n_breaks = conn.execute("SELECT COUNT(*) FROM breaks").fetchone()[0]
    st.metric("Records in DB", f"{n_breaks:,}")


# ─────────────────────────────── helpers ─────────────────────────────────────

def _build_aggrid(
    df: pd.DataFrame,
    height: int = 550,
    extra_cols: Optional[dict] = None,
    hide_cols: Optional[list] = None,
    selection_mode: str = "single",
) -> dict:
    """Build a fully-configured AgGrid with row colouring, filter, sort."""
    gb = GridOptionsBuilder.from_dataframe(df)
    gb.configure_default_column(
        filter=True,
        sortable=True,
        resizable=True,
        minWidth=80,
        wrapText=False,
    )

    # ML columns
    if "false_break_prob" in df.columns:
        gb.configure_column("false_break_prob", type=["numericColumn"],
                            valueFormatter="data.false_break_prob != null ? data.false_break_prob.toFixed(2) : ''",
                            cellStyle=PROB_CELL_STYLE, pinned=None)
    if "risk_tier" in df.columns:
        gb.configure_column("risk_tier", cellRenderer=RISK_TIER_RENDERER,
                            filter="agSetColumnFilter")
    if "dq_score" in df.columns:
        gb.configure_column("dq_score", type=["numericColumn"],
                            valueFormatter="data.dq_score != null ? data.dq_score.toFixed(2) : ''",
                            cellStyle=DQ_CELL_STYLE)

    if hide_cols:
        for col in hide_cols:
            if col in df.columns:
                gb.configure_column(col, hide=True)

    if extra_cols:
        for col, opts in extra_cols.items():
            if col in df.columns:
                gb.configure_column(col, **opts)

    gb.configure_grid_options(rowClassRules=ROW_CLASS_RULES)
    gb.configure_selection(selection_mode=selection_mode, use_checkbox=False)
    gb.configure_pagination(paginationAutoPageSize=False, paginationPageSize=100)

    return AgGrid(
        df,
        gridOptions=gb.build(),
        update_mode=GridUpdateMode.SELECTION_CHANGED,
        allow_unsafe_jscode=True,
        theme="streamlit",
        height=height,
        fit_columns_on_grid_load=False,
    )


def _run_full_pipeline(df: pd.DataFrame) -> pd.DataFrame:
    """DQ score → feature matrix → label → score."""
    df = compute_dq_scores(df)
    feat = build_feature_matrix(df)
    for col in feat.columns:
        if col not in df.columns:
            df[col] = feat[col].values
    df = label_breaks(df)
    df = score_breaks(df, model_dir=MODELS_DIR, threshold=st.session_state.fp_threshold)
    return df


# ─────────────────────────────── Page 1: Break Overview ───────────────────────

def page_break_overview() -> None:
    st.header("Break Overview")

    # ── File Upload ──────────────────────────────────────────────────────────
    uploaded = st.file_uploader(
        "Upload MI Report CSV",
        type=["csv"],
        help="Upload the monthly MI report CSV (41 columns, amounts in pence)",
    )
    if uploaded:
        with st.spinner("Ingesting and scoring..."):
            raw = uploaded.read()
            df, upload_hash = prepare_for_ingest(raw)
            existing = conn.execute(
                "SELECT COUNT(*) FROM upload_log WHERE upload_hash = ?", [upload_hash]
            ).fetchone()[0]
            if existing > 0:
                st.warning("This file was already uploaded (duplicate detected). Re-ingesting.")

            df_scored = _run_full_pipeline(df)
            ingest_dataframe(conn, df_scored)
            log_upload(conn, upload_hash, uploaded.name, len(df_scored))
        st.success(f"Ingested {len(df_scored):,} breaks.")

    # ── Filters ──────────────────────────────────────────────────────────────
    col1, col2 = st.columns([3, 1])
    with col1:
        asset_filter = st.multiselect(
            "Filter by Asset Class",
            VALID_ASSET_CLASSES,
            default=st.session_state.asset_filter or VALID_ASSET_CLASSES,
        )
        st.session_state.asset_filter = asset_filter

    # ── KPI Cards (fragment) ─────────────────────────────────────────────────
    @st.fragment
    def kpi_cards() -> None:
        kpis = get_kpis(conn)
        c1, c2, c3, c4, c5 = st.columns(5)
        c1.metric("Total Breaks",   f"{kpis['total_breaks']:,}")
        c2.metric("Suppressed",     f"{kpis['suppressed']:,}",
                  delta=f"{kpis['suppressed_pct']}%",
                  delta_color="inverse")
        c3.metric("Review",         f"{kpis['review']:,}")
        c4.metric("Escalate",       f"{kpis['escalate']:,}")
        c5.metric("Value Suppressed", f"£{kpis['value_suppressed_gbp']:,.0f}")
        st.caption(f"Avg DQ Score: {kpis['avg_dq_score']:.3f}")

    kpi_cards()

    # ── AgGrid (fragment) ────────────────────────────────────────────────────
    @st.fragment
    def breaks_grid() -> None:
        df = query_breaks(conn, asset_classes=asset_filter if asset_filter else None)
        if df.empty:
            st.info("No break data loaded. Upload an MI report CSV above.")
            return

        hide = [] if st.session_state.debug_mode else ["pseudo_label", "label_confidence"]
        response = _build_aggrid(df, height=550, hide_cols=hide)

        selected = response.get("selected_rows")
        if selected is not None and len(selected) > 0:
            row = selected[0] if isinstance(selected, list) else selected.iloc[0]
            bid = row.get("break_id") or (row["break_id"] if "break_id" in row else None)
            if bid:
                st.session_state.selected_break_id = bid

    breaks_grid()


# ─────────────────────────────── Page 2: ML Scoring ───────────────────────────

def page_ml_scoring() -> None:
    st.header("ML Scoring")

    # ── Threshold Controls (fragment) ────────────────────────────────────────
    @st.fragment
    def threshold_panel() -> None:
        st.subheader("Suppression Threshold")
        advanced = st.toggle("Advanced: per-asset-class thresholds")

        if advanced:
            new_thresholds: dict = {}
            cols = st.columns(4)
            for i, ac in enumerate(VALID_ASSET_CLASSES):
                default_thr = st.session_state.asset_thresholds.get(ac, 0.80)
                new_thresholds[ac] = cols[i].slider(
                    f"{ac} threshold", 0.50, 0.99, float(default_thr), 0.01,
                    key=f"thr_{ac}",
                )
            st.session_state.asset_thresholds = new_thresholds
            update_risk_tiers(conn, asset_thresholds=new_thresholds)
        else:
            thr = st.slider(
                "False Break Suppression Threshold",
                min_value=0.50, max_value=0.99,
                value=float(st.session_state.fp_threshold), step=0.01,
            )
            if abs(thr - st.session_state.fp_threshold) > 1e-4:
                st.session_state.fp_threshold = thr
                update_risk_tiers(conn, threshold=thr)
                st.session_state.asset_thresholds = {}

        # Live suppression stats
        df = query_breaks(conn, columns=["risk_tier", "break_amount_gbp"])
        if not df.empty:
            stats = compute_suppression_stats(df, st.session_state.fp_threshold)
            c1, c2, c3 = st.columns(3)
            c1.metric(
                "Suppressing",
                f"{stats['suppressed']:,} breaks ({stats['suppressed_pct']}%)",
            )
            c2.metric("Saving from MI", f"£{stats['value_suppressed']:,.0f}")
            c3.metric("Threshold", f"{stats['threshold']:.2f}")

    threshold_panel()

    # ── Score distributions (fragment) ───────────────────────────────────────
    @st.fragment
    def score_distributions() -> None:
        st.subheader("Score Distributions by Asset Class")
        df = conn.execute(
            "SELECT asset_class, false_break_prob FROM breaks "
            "WHERE false_break_prob IS NOT NULL"
        ).df()
        if df.empty:
            st.info("No ML scores yet. Upload and score an MI report first.")
            return

        fig = px.histogram(
            df, x="false_break_prob", color="asset_class",
            facet_col="asset_class", nbins=40,
            title="False Break Probability Distribution per Asset Class",
            labels={"false_break_prob": "Probability"},
            color_discrete_sequence=px.colors.qualitative.Set2,
        )
        thr = st.session_state.fp_threshold
        for i in range(len(VALID_ASSET_CLASSES)):
            fig.add_vline(x=thr, line_dash="dash", line_color="red",
                          annotation_text=f"threshold={thr:.2f}", row=1,
                          col=i + 1 if i < 4 else "all")
        st.plotly_chart(fig, use_container_width=True)

    score_distributions()

    # ── Suppression gauge (fragment) ──────────────────────────────────────────
    @st.fragment
    def suppression_gauge() -> None:
        row = conn.execute("""
            SELECT
                COUNT(*) AS total,
                SUM(CASE WHEN risk_tier='Suppress' THEN 1 ELSE 0 END) AS suppressed
            FROM breaks
        """).fetchone()
        total, suppressed = row[0] or 1, row[1] or 0
        pct = suppressed / total * 100

        fig = go.Figure(go.Indicator(
            mode="gauge+number",
            value=pct,
            number={"suffix": "%"},
            title={"text": "Suppression Rate"},
            gauge={
                "axis": {"range": [0, 100]},
                "bar": {"color": "#dc3545"},
                "steps": [
                    {"range": [0, 30],  "color": "#d4edda"},
                    {"range": [30, 60], "color": "#fff3cd"},
                    {"range": [60, 100],"color": "#f8d7da"},
                ],
                "threshold": {
                    "line": {"color": "black", "width": 2},
                    "thickness": 0.75, "value": 50,
                },
            },
        ))
        fig.update_layout(height=300)
        st.plotly_chart(fig, use_container_width=True)

    suppression_gauge()


# ─────────────────────────────── Page 3: Drill-Down ───────────────────────────

def page_drilldown() -> None:
    st.header("Drill-Down Analysis")

    # ── Drill-down AgGrid (fragment) ─────────────────────────────────────────
    @st.fragment
    def drilldown_grid() -> None:
        asset_filter = st.session_state.asset_filter or None
        df = query_breaks(conn, asset_classes=asset_filter)
        if df.empty:
            st.info("No data loaded.")
            return

        hide = [] if st.session_state.debug_mode else ["pseudo_label", "label_confidence"]
        response = _build_aggrid(df, height=500, hide_cols=hide)

        selected = response.get("selected_rows")
        if selected is not None and len(selected) > 0:
            row = selected[0] if isinstance(selected, list) else selected.iloc[0]
            bid = row.get("break_id")
            if bid:
                st.session_state.selected_break_id = bid
                st.success(f"Selected: {bid}")

    drilldown_grid()

    # ── SHAP waterfall panel (fragment) ──────────────────────────────────────
    @st.fragment
    def shap_panel() -> None:
        break_id = st.session_state.selected_break_id
        if not break_id:
            st.info("Click a row in the table above to see its ML explanation.")
            return

        with st.expander("ML Explanation — SHAP Waterfall", expanded=True):
            row_df = conn.execute(
                "SELECT * FROM breaks WHERE break_id = ?", [break_id]
            ).df()
            if row_df.empty:
                st.warning(f"Break {break_id} not found in database.")
                return

            feature_row = row_df.iloc[0]
            prob = feature_row.get("false_break_prob", None)
            ac   = str(feature_row.get("asset_class", ""))

            model, explainer = load_model(ac, MODELS_DIR)
            if explainer is None:
                st.warning(
                    f"No trained model for {ac} yet. "
                    "Train a model from the Feedback & Retraining page first."
                )
                _show_rule_based_explanation(feature_row)
                return

            feature_names = get_feature_names(ac)
            try:
                fig = explain_break(explainer, feature_row, feature_names,
                                    break_id=break_id,
                                    false_break_prob=float(prob) if prob is not None else None)
                st.plotly_chart(fig, use_container_width=True)

                # Top-5 features
                feat_array = [float(feature_row.get(f, np.nan)) for f in feature_names]
                sv = explainer.shap_values(
                    np.array(feat_array, dtype=np.float32).reshape(1, -1)
                )
                if isinstance(sv, list):
                    sv = sv[1][0]
                else:
                    sv = sv[0]
                top5 = get_top_features(sv, feature_names, n=5)
                st.subheader("Top 5 Contributing Features")
                cols = st.columns(5)
                for i, (fname, fval) in enumerate(top5):
                    raw_val = feature_row.get(fname, "N/A")
                    cols[i].metric(
                        label=fname,
                        value=f"{float(raw_val):.3g}" if raw_val != "N/A" else "N/A",
                        delta=f"SHAP: {fval:+.3f}",
                    )
            except Exception as e:
                st.error(f"Could not generate SHAP explanation: {e}")

    shap_panel()


def _show_rule_based_explanation(row: pd.Series) -> None:
    """Fallback: show rule-based signals when no SHAP model is available."""
    st.write("**Rule-Based Signals:**")
    signals = {
        "Auto-close rate (R01)": row.get("rolling_7d_autoclose_rate"),
        "Offsetting break (R02)": row.get("offsetting_break_exists"),
        "Statistical outlier (R03)": row.get("is_statistical_outlier"),
        "Break age (days)": row.get("break_age_days"),
        "Governance score": row.get("governance_completeness_score"),
        "DQ score": row.get("dq_score"),
    }
    for name, val in signals.items():
        if val is not None:
            st.write(f"- **{name}**: {val}")


# ─────────────────────────────── Page 4: Data Quality ─────────────────────────

def page_data_quality() -> None:
    st.header("Data Quality")

    @st.fragment
    def dq_heatmap() -> None:
        df = conn.execute("""
            SELECT
                asset_class,
                ROUND(AVG(dq_score),3)                                               AS avg_dq_score,
                ROUND(AVG(CASE WHEN owner IS NOT NULL THEN 1.0 ELSE 0.0 END),3)      AS owner_pct,
                ROUND(AVG(CASE WHEN approver IS NOT NULL THEN 1.0 ELSE 0.0 END),3)   AS approver_pct,
                ROUND(AVG(CASE WHEN status IS NOT NULL THEN 1.0 ELSE 0.0 END),3)     AS status_pct,
                COUNT(*)                                                               AS row_count
            FROM breaks
            GROUP BY asset_class
            ORDER BY asset_class
        """).df()

        if df.empty:
            st.info("No data loaded yet.")
            return

        col1, col2 = st.columns(2)
        with col1:
            fig = px.bar(
                df, x="asset_class", y="avg_dq_score",
                color="avg_dq_score",
                color_continuous_scale="RdYlGn",
                range_color=[0, 1],
                title="Average DQ Score by Asset Class",
                labels={"avg_dq_score": "DQ Score"},
            )
            fig.update_layout(yaxis_range=[0, 1])
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            melt = df.melt(
                id_vars=["asset_class"],
                value_vars=["owner_pct", "approver_pct", "status_pct"],
                var_name="field", value_name="pct",
            )
            fig2 = px.bar(
                melt, x="asset_class", y="pct", color="field",
                barmode="group",
                title="Governance Field Completeness",
                labels={"pct": "% Populated", "field": "Field"},
                color_discrete_map={
                    "owner_pct":    "#4CAF50",
                    "approver_pct": "#2196F3",
                    "status_pct":   "#FF9800",
                },
            )
            fig2.update_layout(yaxis_range=[0, 1])
            st.plotly_chart(fig2, use_container_width=True)

        st.dataframe(df, use_container_width=True)

    dq_heatmap()

    @st.fragment
    def low_dq_grid() -> None:
        st.subheader("Lowest DQ Score Breaks (bottom 10%)")
        df = conn.execute("""
            SELECT break_id, asset_class, entity_id, break_amount_gbp,
                   dq_score, risk_tier, false_break_prob,
                   owner, approver, status
            FROM breaks
            WHERE dq_score IS NOT NULL
            ORDER BY dq_score ASC
            LIMIT 500
        """).df()
        if df.empty:
            return
        _build_aggrid(df, height=400)

    low_dq_grid()


# ─────────────────────────────── Page 5: Feedback & Retraining ────────────────

def page_feedback() -> None:
    st.header("Feedback & Retraining")

    @st.fragment
    def feedback_form() -> None:
        st.subheader("Submit Analyst Feedback")
        options = conn.execute(
            "SELECT break_id, asset_class, false_break_prob, risk_tier "
            "FROM breaks WHERE false_break_prob IS NOT NULL "
            "ORDER BY false_break_prob DESC LIMIT 500"
        ).df()

        if options.empty:
            st.info("No scored breaks available. Upload and score data first.")
            return

        opt_labels = [
            f"{r['break_id']}  [{r['asset_class']}]  prob={r['false_break_prob']:.2f}  tier={r['risk_tier']}"
            for _, r in options.iterrows()
        ]
        selected_idx = st.selectbox("Select Break", range(len(opt_labels)),
                                    format_func=lambda i: opt_labels[i])
        sel_row = options.iloc[selected_idx]

        st.info(
            f"**Break ID**: {sel_row['break_id']} | "
            f"**Asset Class**: {sel_row['asset_class']} | "
            f"**False Break Prob**: {sel_row['false_break_prob']:.3f} | "
            f"**Risk Tier**: {sel_row['risk_tier']}"
        )

        analyst_id = st.text_input("Analyst ID", placeholder="e.g. jsmith")
        true_label = st.radio(
            "True Label",
            ["Genuine Break", "False Break"],
            horizontal=True,
        )
        label_int    = 1 if true_label == "False Break" else 0
        override_flag = (sel_row["risk_tier"] == "Suppress") and (label_int == 0)

        if st.button("Submit Feedback", type="primary"):
            if not analyst_id:
                st.error("Please enter your Analyst ID.")
                return
            report_month = conn.execute(
                "SELECT report_month FROM breaks WHERE break_id = ?", [sel_row["break_id"]]
            ).fetchone()
            rm = report_month[0] if report_month else datetime.now().strftime("%Y-%m")

            ok = save_feedback(
                conn,
                break_id       = str(sel_row["break_id"]),
                analyst_label  = label_int,
                analyst_id     = analyst_id,
                model_prediction = float(sel_row["false_break_prob"]),
                asset_class    = str(sel_row["asset_class"]),
                report_month   = rm,
                override_flag  = bool(override_flag),
            )
            if ok:
                st.toast(f"Feedback saved for {sel_row['break_id']}!", icon="✅")
            else:
                st.error("Failed to save feedback. Check logs.")

    feedback_form()

    @st.fragment
    def feedback_metrics() -> None:
        st.subheader("Feedback Progress")
        counts = get_feedback_count(conn)
        cols = st.columns(4)
        for i, ac in enumerate(VALID_ASSET_CLASSES):
            ac_counts = counts.get(ac, {})
            total_ac  = ac_counts.get("total", 0)
            cols[i].metric(
                label=ac,
                value=f"{total_ac} labels",
                delta=f"{ac_counts.get('false_break',0)} FB / {ac_counts.get('genuine',0)} Gen",
            )
            cols[i].progress(min(total_ac / 200, 1.0), text=f"{total_ac}/200")

        total = counts.get("TOTAL", {}).get("total", 0)
        st.caption(f"Total labels collected: {total} (need 200+ per class to retrain)")

    feedback_metrics()

    @st.fragment
    def retrain_section() -> None:
        st.subheader("Model Retraining")
        st.info(f"Current model version: **{st.session_state.model_version}**")

        counts = get_feedback_count(conn)
        total  = counts.get("TOTAL", {}).get("total", 0)
        can_retrain = total >= 200

        if st.button(
            "Retrain Model",
            disabled=not can_retrain,
            type="primary",
            help="Requires 200+ analyst labels across all asset classes",
        ):
            with st.spinner("Training models for all asset classes..."):
                trained = []
                df_all = conn.execute("SELECT * FROM breaks").df()
                fb_df  = load_feedback(conn)

                for ac in VALID_ASSET_CLASSES:
                    result = train_from_dataframe(df_all, asset_class=ac,
                                                  model_dir=MODELS_DIR)
                    if result:
                        _, _, metrics = result
                        trained.append(
                            f"{ac}: AUC-ROC={metrics.get('cv_mean_auc_roc', 0):.3f}"
                        )

            if trained:
                version = datetime.now().strftime("v%Y_%m")
                st.session_state.model_version = version
                st.success(f"Models trained ({version}):\n" + "\n".join(trained))
            else:
                st.warning("No asset classes had sufficient data for training.")

    retrain_section()

    # Feedback history table
    @st.fragment
    def feedback_history() -> None:
        st.subheader("Feedback History")
        df = load_feedback(conn)
        if df.empty:
            st.info("No feedback submitted yet.")
            return
        _build_aggrid(df, height=300)

    feedback_history()


# ─────────────────────────────── Page 6: Export ───────────────────────────────

def page_export() -> None:
    st.header("Export")

    @st.fragment
    def export_mi_report() -> None:
        st.subheader("MI Report (Genuine + Review breaks)")
        df = conn.execute(
            "SELECT * FROM breaks WHERE risk_tier IN ('Escalate', 'Review')"
        ).df()

        if df.empty:
            st.info("No Escalate/Review breaks to export.")
            return

        st.metric("Rows in MI export", f"{len(df):,}")

        # Build CSV with metadata header
        threshold   = st.session_state.fp_threshold
        n_suppressed = conn.execute(
            "SELECT COUNT(*) FROM breaks WHERE risk_tier='Suppress'"
        ).fetchone()[0]
        meta = (
            f"# CASHFOARE MI Report Export\n"
            f"# model_version={st.session_state.model_version}\n"
            f"# suppression_threshold={threshold:.2f}\n"
            f"# n_suppressed={n_suppressed}\n"
            f"# n_genuine_review={len(df)}\n"
            f"# report_generated={datetime.utcnow().isoformat()}Z\n"
        )
        csv_bytes  = (meta + df.to_csv(index=False)).encode("utf-8")
        st.download_button(
            "Download MI Report (CSV)",
            data=csv_bytes,
            file_name="mi_report.csv",
            mime="text/csv",
        )

    export_mi_report()

    @st.fragment
    def export_suppressed() -> None:
        st.subheader("Suppressed Breaks Audit Trail")
        df = conn.execute("""
            SELECT
                break_id,
                asset_class,
                false_break_prob,
                risk_tier,
                CAST(NOW() AS VARCHAR) AS suppression_timestamp,
                dq_score,
                mad_score,
                governance_completeness_score,
                report_month
            FROM breaks
            WHERE risk_tier = 'Suppress'
        """).df()

        if df.empty:
            st.info("No suppressed breaks to export.")
            return

        st.metric("Suppressed breaks", f"{len(df):,}")
        ym = datetime.now().strftime("%Y_%m")
        filename = f"suppressed_breaks_{ym}.csv"

        csv_bytes = df.to_csv(index=False).encode("utf-8")

        # Also write to disk (audit trail requirement)
        Path("exports").mkdir(exist_ok=True)
        with open(f"exports/{filename}", "wb") as f:
            f.write(csv_bytes)

        st.download_button(
            f"Download {filename}",
            data=csv_bytes,
            file_name=filename,
            mime="text/csv",
        )
        st.caption(f"Also saved to: exports/{filename}")

    export_suppressed()


# ─────────────────────────────── page router ─────────────────────────────────

if page == "Break Overview":
    page_break_overview()
elif page == "ML Scoring":
    page_ml_scoring()
elif page == "Drill-Down":
    page_drilldown()
elif page == "Data Quality":
    page_data_quality()
elif page == "Feedback & Retraining":
    page_feedback()
elif page == "Export":
    page_export()
