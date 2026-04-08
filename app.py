"""
CASHFOARE Phase 1 — Break Lifecycle & Rec Flow Dashboard
=========================================================
Streamlit app: upload Cash + MI CSVs → Phase 1 pipeline → KPIs + AgGrid MI

Run:
    streamlit run app.py

Requires:
    pip install streamlit plotly pandas streamlit-aggrid
"""

from __future__ import annotations

import io
import sys
from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode, JsCode

# ── import pipeline functions from scripts/phase1_pipeline.py ─────────────────
sys.path.insert(0, str(Path(__file__).resolve().parent))
from scripts.phase1_pipeline import (
    DEFAULT_END,
    DEFAULT_START,
    MAX_AGE_DAYS,
    RECURRING_MIN_DAYS,
    ROOT_SYSTEM_MAP,
    build_break_chains,
    build_chain_summary,
    build_final_summary,
    build_journal_flags,
    build_rec_flow,
    filter_date_range,
    standardize_cash,
    standardize_mi,
)

# ─── Page config ──────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="CASHFOARE Phase 1 — Break Lifecycle",
    page_icon="🔍",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ─── Global CSS (dark theme) ──────────────────────────────────────────────────
st.markdown("""
<style>
.stApp { background-color: #080d1a; }
.stApp > header { background-color: #080d1a; }
section[data-testid="stSidebar"] {
    background-color: #0a0f1e;
    border-right: 1px solid #1e2d4a;
}
section[data-testid="stSidebar"] * { color: #94a3b8 !important; }
.stTabs [data-baseweb="tab-list"] {
    background-color: #0f1525;
    border-radius: 8px;
    padding: 4px;
}
.stTabs [data-baseweb="tab"] {
    background-color: transparent;
    border-radius: 6px;
    color: #64748b;
    font-weight: 700;
    font-size: 11px;
    letter-spacing: 1px;
}
.stTabs [aria-selected="true"] {
    background-color: #1e2d4a !important;
    color: #f59e0b !important;
}
[data-testid="stMetric"] {
    background: #0f1525;
    border: 1px solid #1e2d4a;
    border-radius: 8px;
    padding: 12px 16px;
}
[data-testid="stMetricValue"] { color: #f1f5f9 !important; font-size: 26px !important; font-weight: 800 !important; }
[data-testid="stMetricLabel"] { color: #64748b !important; font-size: 11px !important; }
h1, h2, h3 { color: #f1f5f9 !important; }
p, label, .stMarkdown { color: #94a3b8 !important; }
.stSelectbox > div > div { background: #0f1525 !important; border-color: #1e2d4a !important; }
hr { border-color: #1e2d4a; }
.streamlit-expanderHeader { background: #0f1525 !important; color: #94a3b8 !important; }
.streamlit-expanderContent { background: #080d1a !important; border: 1px solid #1e2d4a !important; }
.stButton > button {
    background: #0f1525; border: 1px solid #1e2d4a;
    color: #94a3b8; border-radius: 6px; font-weight: 700;
}
.stButton > button:hover { border-color: #f59e0b; color: #f59e0b; }
.stFileUploader { background: #0f1525; border: 1px dashed #1e2d4a; border-radius: 8px; }
</style>
""", unsafe_allow_html=True)

# ─── AgGrid JS renderers ──────────────────────────────────────────────────────

BOOL_RENDERER = JsCode("""
function(params) {
    if (params.value === true || params.value === 'True') {
        return '<span style="background:#ef444422;border:1px solid #ef4444;color:#ef4444;'
             + 'padding:1px 8px;border-radius:3px;font-size:11px;font-weight:800;">YES</span>';
    }
    return '<span style="background:#10b98122;border:1px solid #10b981;color:#10b981;'
         + 'padding:1px 8px;border-radius:3px;font-size:11px;">no</span>';
}
""")

ROOT_SYSTEM_RENDERER = JsCode("""
function(params) {
    const colours = {
        FOBO:'#3b82f6', Fidessa:'#0ea5e9', FLARE:'#10b981',
        Netted:'#06b6d4', COMPTA:'#8b5cf6', Sophis:'#a78bfa',
        'P&L':'#f59e0b', Washbook:'#64748b'
    };
    const v = params.value || 'Unknown';
    const c = colours[v] || '#64748b';
    return `<span style="background:${c}22;border:1px solid ${c}66;color:${c};`
         + `padding:1px 8px;border-radius:3px;font-size:11px;font-weight:700;">${v}</span>`;
}
""")

ROW_CLASS_RULES = {
    "row-false-closure": "data.False_Closure === true || data.False_Closure === 'True'",
    "row-recurring":     "(data.Is_Recurring === true || data.Is_Recurring === 'True') && !data.False_Closure",
}

AGGRID_ROW_CSS = """
<style>
.ag-theme-streamlit .row-false-closure { background-color: #3a0d0d !important; }
.ag-theme-streamlit .row-recurring     { background-color: #2a2200 !important; }
</style>
"""

# ─── Caching ──────────────────────────────────────────────────────────────────

@st.cache_data(show_spinner=False)
def run_pipeline(
    cash_bytes: bytes,
    mi_bytes:   bytes,
    start_str:  str,
    end_str:    str,
) -> pd.DataFrame:
    """Cache by file content + date range. Re-runs only when inputs change."""
    start_ts = pd.Timestamp(start_str)
    end_ts   = pd.Timestamp(end_str)

    raw_cash = pd.read_csv(io.BytesIO(cash_bytes))
    raw_mi   = pd.read_csv(io.BytesIO(mi_bytes))

    cash_df = standardize_cash(raw_cash)
    mi_df   = standardize_mi(raw_mi)

    cash_df = filter_date_range(cash_df, "Date", start_ts, end_ts)
    mi_df   = filter_date_range(mi_df,   "Date", start_ts, end_ts)

    if "Age Days" in mi_df.columns:
        mi_df = mi_df[mi_df["Age Days"] <= MAX_AGE_DAYS]

    cash_df       = build_break_chains(cash_df)
    chain_summary = build_chain_summary(cash_df)
    rec_flow_df, root_df = build_rec_flow(mi_df)
    journal_df    = build_journal_flags(mi_df)

    return build_final_summary(chain_summary, rec_flow_df, root_df, journal_df)


# ─── AgGrid builder ───────────────────────────────────────────────────────────

def build_aggrid(df: pd.DataFrame, height: int = 560) -> dict:
    gb = GridOptionsBuilder.from_dataframe(df)
    gb.configure_default_column(
        filter=True, sortable=True, resizable=True, minWidth=90,
    )

    # Bool columns
    for col in ("Is_Recurring", "Journal_Used", "False_Closure"):
        if col in df.columns:
            gb.configure_column(col, cellRenderer=BOOL_RENDERER,
                                filter="agSetColumnFilter", width=130)

    # Root system chip
    if "Root_System" in df.columns:
        gb.configure_column("Root_System", cellRenderer=ROOT_SYSTEM_RENDERER,
                            filter="agSetColumnFilter", width=120)

    # Numeric formatting
    for col in ("Total_Amount",):
        if col in df.columns:
            gb.configure_column(
                col,
                type=["numericColumn"],
                valueFormatter="data.Total_Amount != null ? '£' + data.Total_Amount.toLocaleString('en-GB', {maximumFractionDigits:0}) : ''",
                width=130,
            )
    for col in ("Days_Active", "Total_Breaks", "BreakChainID"):
        if col in df.columns:
            gb.configure_column(col, type=["numericColumn"], width=110)

    # Wide columns
    for col in ("Rec_Flow",):
        if col in df.columns:
            gb.configure_column(col, width=380, tooltipField=col)

    gb.configure_grid_options(rowClassRules=ROW_CLASS_RULES)
    gb.configure_selection("single", use_checkbox=False)
    gb.configure_pagination(paginationAutoPageSize=False, paginationPageSize=50)

    return AgGrid(
        df,
        gridOptions=gb.build(),
        update_mode=GridUpdateMode.SELECTION_CHANGED,
        allow_unsafe_jscode=True,
        theme="streamlit",
        height=height,
        fit_columns_on_grid_load=False,
    )


# ─── KPI helpers ─────────────────────────────────────────────────────────────

def kpi_delta(n: int, total: int) -> str:
    return f"{n / total * 100:.1f}%" if total else "—"


# ─── Sidebar ──────────────────────────────────────────────────────────────────

with st.sidebar:
    st.markdown("""
    <div style='text-align:center;padding:12px 0 8px;'>
      <div style='color:#f59e0b;font-size:12px;font-weight:800;letter-spacing:2px;'>CASHFOARE</div>
      <div style='color:#64748b;font-size:9px;letter-spacing:1px;'>Phase 1 · Break Lifecycle</div>
    </div>
    """, unsafe_allow_html=True)
    st.divider()

    st.markdown("<div style='color:#64748b;font-size:9px;font-weight:700;letter-spacing:2px;margin-bottom:6px;'>DATA UPLOAD</div>",
                unsafe_allow_html=True)
    cash_file = st.file_uploader("Cash breaks CSV", type=["csv"], key="cash_upload",
                                  help="Daily CASHFOARE breaks file (BREAK_DATE, TRADE_ID, PRODUCT, BREAK columns)")
    mi_file   = st.file_uploader("MI report CSV",   type=["csv"], key="mi_upload",
                                  help="Monthly MI report with Rec Name, TRADE REF, Date columns")

    st.divider()
    st.markdown("<div style='color:#64748b;font-size:9px;font-weight:700;letter-spacing:2px;margin-bottom:6px;'>DATE RANGE</div>",
                unsafe_allow_html=True)
    col_s, col_e = st.columns(2)
    start_date = col_s.date_input("From", value=DEFAULT_START.date(), key="start_date")
    end_date   = col_e.date_input("To",   value=DEFAULT_END.date(),   key="end_date")

    st.divider()
    st.markdown("<div style='color:#64748b;font-size:9px;font-weight:700;letter-spacing:2px;margin-bottom:4px;'>FILTERS</div>",
                unsafe_allow_html=True)
    show_recurring_only  = st.checkbox("Recurring only",     value=False)
    show_false_closure   = st.checkbox("False Closure only", value=False)

    st.divider()
    st.markdown("<div style='color:#64748b;font-size:9px;'>ACT · OMRC AI/ML · April 2026</div>",
                unsafe_allow_html=True)

# ─── Header ───────────────────────────────────────────────────────────────────

st.markdown("""
<div style='margin-bottom:4px;'>
  <span style='background:#f59e0b22;border:1px solid #f59e0b55;color:#f59e0b;
        border-radius:4px;padding:2px 10px;font-size:10px;font-weight:800;letter-spacing:2px;'>
    CASHFOARE · PHASE 1 · BREAK LIFECYCLE
  </span>
</div>
<h1 style='color:#f1f5f9;font-size:24px;font-weight:900;margin:4px 0 2px;'>
  Break Lifecycle &amp; Rec Flow Analysis
</h1>
<p style='color:#475569;font-size:12px;margin:0 0 16px;'>
  Lifecycle · Rec flow · Journal flag · False closure detection
</p>
""", unsafe_allow_html=True)

# ─── Require both files ───────────────────────────────────────────────────────

if not cash_file or not mi_file:
    st.markdown("""
    <div style='background:#0f1525;border:1px dashed #1e2d4a;border-radius:10px;
                padding:40px;text-align:center;margin-top:20px;'>
      <div style='font-size:32px;margin-bottom:12px;'>📂</div>
      <div style='color:#94a3b8;font-size:14px;font-weight:700;'>Upload both CSV files in the sidebar to begin</div>
      <div style='color:#475569;font-size:11px;margin-top:6px;'>Cash breaks CSV  +  MI report CSV</div>
    </div>
    """, unsafe_allow_html=True)
    st.stop()

# ─── Run pipeline ─────────────────────────────────────────────────────────────

with st.spinner("Running Phase 1 pipeline…"):
    try:
        cash_bytes = cash_file.read()
        mi_bytes   = mi_file.read()
        df = run_pipeline(
            cash_bytes, mi_bytes,
            str(start_date), str(end_date),
        )
    except Exception as exc:
        st.error(f"Pipeline error — check your CSV columns match the expected format.\n\n{exc}")
        st.stop()

# ─── Apply sidebar filters ────────────────────────────────────────────────────

df_view = df.copy()
if show_recurring_only:
    df_view = df_view[df_view["Is_Recurring"]]
if show_false_closure:
    df_view = df_view[df_view["False_Closure"]]

# ─── KPI row ──────────────────────────────────────────────────────────────────

total         = len(df)
recurring     = int(df["Is_Recurring"].sum())
journal_used  = int(df["Journal_Used"].sum())
false_closure = int(df["False_Closure"].sum())
total_amount  = df["Total_Amount"].sum()

k1, k2, k3, k4, k5 = st.columns(5)
k1.metric("Total Chains",    f"{total:,}")
k2.metric("Recurring",       f"{recurring:,}",     delta=kpi_delta(recurring,     total))
k3.metric("Journal Used",    f"{journal_used:,}",  delta=kpi_delta(journal_used,  total))
k4.metric("False Closure",   f"{false_closure:,}", delta=kpi_delta(false_closure, total),
          delta_color="inverse")
k5.metric("Total Break £",   f"£{total_amount:,.0f}")

st.divider()

# ─── Tabs ─────────────────────────────────────────────────────────────────────

tab1, tab2, tab3 = st.tabs([
    "  Break Chain MI",
    "  Root System Analysis",
    "  False Closure Detail",
])

# ══════════════════════════════════════════════════════════════════════════════
# TAB 1 — Break Chain MI (AgGrid)
# ══════════════════════════════════════════════════════════════════════════════

with tab1:
    c_info, c_export = st.columns([5, 1])
    with c_info:
        n_shown = len(df_view)
        st.markdown(
            f"<div style='color:#64748b;font-size:10px;padding:6px 0;'>"
            f"Showing <b style='color:#f1f5f9;'>{n_shown:,}</b> chains"
            f"{' (filtered)' if n_shown < total else ''}"
            f" · <span style='color:#ef4444;'>■</span> False Closure &nbsp;"
            f"<span style='color:#f59e0b;'>■</span> Recurring</div>",
            unsafe_allow_html=True,
        )
    with c_export:
        csv_bytes = df_view.to_csv(index=False).encode("utf-8")
        st.download_button(
            "⬇ Export CSV",
            data=csv_bytes,
            file_name="phase1_break_chains.csv",
            mime="text/csv",
            use_container_width=True,
        )

    st.markdown(AGGRID_ROW_CSS, unsafe_allow_html=True)

    if df_view.empty:
        st.info("No chains match the current filters.")
    else:
        # Column order for display
        display_cols = [c for c in [
            "Trade", "Product", "Start_Date", "End_Date",
            "Days_Active", "Total_Breaks", "Total_Amount",
            "Is_Recurring", "Root_System", "Root_Rec",
            "Rec_Flow", "Journal_Used", "False_Closure",
            "BreakChainID",
        ] if c in df_view.columns]

        build_aggrid(df_view[display_cols], height=580)

# ══════════════════════════════════════════════════════════════════════════════
# TAB 2 — Root System Analysis
# ══════════════════════════════════════════════════════════════════════════════

with tab2:
    if df.empty:
        st.info("No data to analyse.")
    else:
        root_summary = (
            df.groupby("Root_System", dropna=False)
            .agg(
                Total_Chains    = ("BreakChainID",  "count"),
                False_Closures  = ("False_Closure", "sum"),
                Recurring       = ("Is_Recurring",  "sum"),
                Journal_Used    = ("Journal_Used",  "sum"),
                Total_Amount    = ("Total_Amount",  "sum"),
            )
            .reset_index()
            .sort_values("Total_Chains", ascending=False)
            .fillna({"Root_System": "Unknown"})
        )
        root_summary["FC_Rate"] = (
            root_summary["False_Closures"] / root_summary["Total_Chains"].clip(lower=1) * 100
        ).round(1)

        col_bar, col_fc = st.columns(2)

        with col_bar:
            fig = px.bar(
                root_summary,
                x="Root_System", y="Total_Chains",
                color="Root_System",
                text="Total_Chains",
                title="Total Break Chains by Root System",
                labels={"Total_Chains": "Chains", "Root_System": ""},
                color_discrete_sequence=px.colors.qualitative.Set2,
            )
            fig.update_traces(textposition="outside")
            fig.update_layout(
                paper_bgcolor="#0f1525", plot_bgcolor="#0f1525",
                font=dict(color="#94a3b8", size=11),
                showlegend=False,
                xaxis=dict(gridcolor="#1e2d4a", tickfont=dict(color="#94a3b8")),
                yaxis=dict(gridcolor="#1e2d4a", tickfont=dict(color="#94a3b8")),
                height=340, margin=dict(l=10, r=10, t=40, b=10),
            )
            st.plotly_chart(fig, use_container_width=True)

        with col_fc:
            fig2 = go.Figure(data=[
                go.Bar(name="False Closures", x=root_summary["Root_System"],
                       y=root_summary["False_Closures"], marker_color="#ef4444"),
                go.Bar(name="Recurring",      x=root_summary["Root_System"],
                       y=root_summary["Recurring"],      marker_color="#f59e0b"),
                go.Bar(name="Journal Used",   x=root_summary["Root_System"],
                       y=root_summary["Journal_Used"],   marker_color="#3b82f6"),
            ])
            fig2.update_layout(
                barmode="group",
                title="False Closure / Recurring / Journal by Root System",
                paper_bgcolor="#0f1525", plot_bgcolor="#0f1525",
                font=dict(color="#94a3b8", size=11),
                legend=dict(bgcolor="#0f1525", bordercolor="#1e2d4a", borderwidth=1,
                            font=dict(color="#94a3b8")),
                xaxis=dict(gridcolor="#1e2d4a", tickfont=dict(color="#94a3b8")),
                yaxis=dict(gridcolor="#1e2d4a", tickfont=dict(color="#94a3b8"),
                           title="Count"),
                height=340, margin=dict(l=10, r=10, t=40, b=10),
            )
            st.plotly_chart(fig2, use_container_width=True)

        # Summary table
        st.markdown("<div style='color:#64748b;font-size:9px;font-weight:700;letter-spacing:2px;margin:8px 0 4px;'>ROOT SYSTEM SUMMARY</div>",
                    unsafe_allow_html=True)
        st.dataframe(
            root_summary.style
            .format({"Total_Amount": "£{:,.0f}", "FC_Rate": "{:.1f}%"})
            .background_gradient(subset=["False_Closures"], cmap="Reds")
            .background_gradient(subset=["Total_Chains"],   cmap="Blues"),
            use_container_width=True,
            height=280,
        )

# ══════════════════════════════════════════════════════════════════════════════
# TAB 3 — False Closure Detail
# ══════════════════════════════════════════════════════════════════════════════

with tab3:
    fc_df = df[df["False_Closure"]].sort_values("Days_Active", ascending=False)

    if fc_df.empty:
        st.markdown("""
        <div style='background:#0f1525;border:1px solid #10b98133;border-radius:8px;
                    padding:24px;text-align:center;'>
          <div style='color:#10b981;font-size:13px;font-weight:700;'>✓ No False Closures detected</div>
          <div style='color:#475569;font-size:11px;margin-top:4px;'>
              All journalled recurring breaks cleared within the date range.
          </div>
        </div>
        """, unsafe_allow_html=True)
    else:
        st.markdown(
            f"<div style='color:#ef4444;font-size:11px;font-weight:700;margin-bottom:8px;'>"
            f"⚠ {len(fc_df)} False Closure chain(s) detected — journalled breaks that recurred</div>",
            unsafe_allow_html=True,
        )

        # Top-10 by Days_Active
        top_fc = fc_df[["Trade", "Product", "Days_Active", "Total_Breaks",
                         "Total_Amount", "Root_System", "Root_Rec", "Rec_Flow"]].head(20)

        st.markdown(AGGRID_ROW_CSS, unsafe_allow_html=True)

        gb_fc = GridOptionsBuilder.from_dataframe(top_fc)
        gb_fc.configure_default_column(filter=True, sortable=True, resizable=True, minWidth=90)
        gb_fc.configure_column("Rec_Flow", width=380, tooltipField="Rec_Flow")
        gb_fc.configure_column("Root_System", cellRenderer=ROOT_SYSTEM_RENDERER,
                               filter="agSetColumnFilter", width=120)
        gb_fc.configure_column("Total_Amount",
                               type=["numericColumn"],
                               valueFormatter="'£' + (data.Total_Amount || 0).toLocaleString('en-GB', {maximumFractionDigits:0})",
                               width=130)
        gb_fc.configure_column("Days_Active",  type=["numericColumn"], width=110)
        gb_fc.configure_column("Total_Breaks", type=["numericColumn"], width=110)
        gb_fc.configure_pagination(paginationAutoPageSize=False, paginationPageSize=20)

        AgGrid(
            top_fc,
            gridOptions=gb_fc.build(),
            update_mode=GridUpdateMode.NO_UPDATE,
            allow_unsafe_jscode=True,
            theme="streamlit",
            height=420,
            fit_columns_on_grid_load=False,
        )

        # Days Active distribution
        fig3 = px.histogram(
            fc_df, x="Days_Active", nbins=20,
            title="False Closure — Days Active Distribution",
            labels={"Days_Active": "Days Active", "count": "Chains"},
            color_discrete_sequence=["#ef4444"],
        )
        fig3.update_layout(
            paper_bgcolor="#0f1525", plot_bgcolor="#0f1525",
            font=dict(color="#94a3b8", size=11),
            xaxis=dict(gridcolor="#1e2d4a", tickfont=dict(color="#94a3b8")),
            yaxis=dict(gridcolor="#1e2d4a", tickfont=dict(color="#94a3b8")),
            height=260, margin=dict(l=10, r=10, t=40, b=10),
        )
        st.plotly_chart(fig3, use_container_width=True)
