import React, { useState, useEffect, useCallback } from "react";
import {
  BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer, Cell,
} from "recharts";
import { CheckCircle, XCircle, AlertTriangle, HelpCircle } from "lucide-react";
import { getDailyInference, getInferenceSummary, submitFeedback } from "../api.js";

const today = new Date().toISOString().split("T")[0];

const PRED_COLORS = { R: "#ef4444", A: "#f59e0b", G: "#22c55e", "no-prediction": "#94a3b8", unknown: "#64748b" };

function PredBadge({ pred }) {
  const color = PRED_COLORS[pred] || "#64748b";
  return (
    <span style={{ background: color + "22", color, border: `1px solid ${color}44`, borderRadius: 4, padding: "2px 8px", fontSize: 12, fontWeight: 600 }}>
      {pred || "—"}
    </span>
  );
}

function ConfBar({ value }) {
  if (value == null) return <span style={{ color: "#475569", fontSize: 12 }}>—</span>;
  const pct = Math.round(value * 100);
  const color = pct >= 70 ? "#22c55e" : pct >= 40 ? "#f59e0b" : "#ef4444";
  return (
    <div style={{ display: "flex", alignItems: "center", gap: 6 }}>
      <div style={{ width: 60, height: 6, background: "#334155", borderRadius: 3 }}>
        <div style={{ width: `${pct}%`, height: "100%", background: color, borderRadius: 3 }} />
      </div>
      <span style={{ fontSize: 12, color }}>{pct}%</span>
    </div>
  );
}

function FeedbackRow({ row, onFeedback }) {
  const [mode, setMode] = useState(null); // null | 'reject'
  const [actualLabel, setActualLabel] = useState("");
  const [done, setDone] = useState(null);

  const handleApprove = async () => {
    await onFeedback({ trade_ref: row.trade_ref, rec_id: row.rec_id, break_id: row.id, predicted_label: row.ml_rag_prediction, feedback_type: "approve", model_type: "rag" });
    setDone("approved");
  };

  const handleRejectSubmit = async () => {
    if (!actualLabel) return;
    await onFeedback({ trade_ref: row.trade_ref, rec_id: row.rec_id, break_id: row.id, predicted_label: row.ml_rag_prediction, actual_label: actualLabel, feedback_type: "reject", model_type: "rag" });
    setDone("rejected");
    setMode(null);
  };

  if (done) {
    return (
      <span style={{ fontSize: 12, color: done === "approved" ? "#22c55e" : "#f87171" }}>
        {done === "approved" ? "✓ Approved" : "✗ Rejected"}
      </span>
    );
  }

  return (
    <div style={{ display: "flex", alignItems: "center", gap: 6 }}>
      {mode !== "reject" && (
        <>
          <button onClick={handleApprove} title="Approve prediction"
            style={{ background: "#16a34a22", border: "1px solid #16a34a44", color: "#22c55e", borderRadius: 4, padding: "3px 10px", cursor: "pointer", fontSize: 12 }}>
            Approve
          </button>
          <button onClick={() => setMode("reject")} title="Reject prediction"
            style={{ background: "#dc262622", border: "1px solid #dc262644", color: "#f87171", borderRadius: 4, padding: "3px 10px", cursor: "pointer", fontSize: 12 }}>
            Reject
          </button>
        </>
      )}
      {mode === "reject" && (
        <div style={{ display: "flex", alignItems: "center", gap: 6 }}>
          <select value={actualLabel} onChange={(e) => setActualLabel(e.target.value)}
            style={{ background: "#1e293b", color: "#cbd5e1", border: "1px solid #475569", borderRadius: 4, padding: "3px 8px", fontSize: 12 }}>
            <option value="">Correct label…</option>
            <option value="R">R – Red</option>
            <option value="A">A – Amber</option>
            <option value="G">G – Green</option>
          </select>
          <button onClick={handleRejectSubmit} disabled={!actualLabel}
            style={{ background: "#7c3aed22", border: "1px solid #7c3aed44", color: "#a78bfa", borderRadius: 4, padding: "3px 10px", cursor: "pointer", fontSize: 12, opacity: actualLabel ? 1 : 0.5 }}>
            Submit
          </button>
          <button onClick={() => setMode(null)}
            style={{ background: "transparent", border: "none", color: "#94a3b8", cursor: "pointer", fontSize: 12 }}>
            Cancel
          </button>
        </div>
      )}
    </div>
  );
}

const s = {
  page: { color: "#e2e8f0", fontFamily: "inherit" },
  header: { display: "flex", alignItems: "center", gap: 16, marginBottom: 20, flexWrap: "wrap" },
  title: { fontSize: 20, fontWeight: 700, color: "#f1f5f9", margin: 0 },
  tabs: { display: "flex", gap: 0, borderBottom: "1px solid #334155", marginBottom: 20 },
  tab: (active) => ({
    padding: "8px 20px", cursor: "pointer", fontSize: 14, fontWeight: active ? 600 : 400,
    color: active ? "#38bdf8" : "#94a3b8",
    borderBottom: active ? "2px solid #38bdf8" : "2px solid transparent",
    background: "transparent", border: "none", outline: "none",
  }),
  input: { background: "#1e293b", color: "#cbd5e1", border: "1px solid #475569", borderRadius: 6, padding: "6px 10px", fontSize: 13 },
  card: { background: "#1e293b", borderRadius: 8, padding: 16, border: "1px solid #334155" },
  grid: { display: "grid", gridTemplateColumns: "repeat(auto-fill, minmax(180px, 1fr))", gap: 12, marginBottom: 20 },
  kpi: { background: "#0f172a", borderRadius: 8, padding: "14px 16px", border: "1px solid #334155" },
  kpiLabel: { fontSize: 11, color: "#64748b", textTransform: "uppercase", letterSpacing: 0.5 },
  kpiValue: { fontSize: 22, fontWeight: 700, color: "#38bdf8", margin: "4px 0 0" },
  table: { width: "100%", borderCollapse: "collapse", fontSize: 13 },
  th: { padding: "8px 10px", textAlign: "left", color: "#64748b", borderBottom: "1px solid #334155", fontSize: 12, fontWeight: 600 },
  td: { padding: "8px 10px", borderBottom: "1px solid #1e293b", color: "#cbd5e1", verticalAlign: "middle" },
  pagination: { display: "flex", gap: 8, alignItems: "center", marginTop: 12, justifyContent: "flex-end" },
  btn: { background: "#1e293b", border: "1px solid #334155", color: "#94a3b8", borderRadius: 4, padding: "4px 12px", cursor: "pointer", fontSize: 13 },
};

export default function Inferencing() {
  const [tab, setTab] = useState("breaks");
  const [date, setDate] = useState(today);
  const [rows, setRows] = useState([]);
  const [total, setTotal] = useState(0);
  const [page, setPage] = useState(1);
  const [summary, setSummary] = useState(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);

  const PAGE_SIZE = 50;

  const loadBreaks = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const data = await getDailyInference(date, page, PAGE_SIZE);
      setRows(data.rows || []);
      setTotal(data.total || 0);
    } catch (e) {
      setError(e.message);
    } finally {
      setLoading(false);
    }
  }, [date, page]);

  const loadSummary = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const data = await getInferenceSummary(date);
      setSummary(data);
    } catch (e) {
      setError(e.message);
    } finally {
      setLoading(false);
    }
  }, [date]);

  useEffect(() => {
    if (tab === "breaks") loadBreaks();
    else loadSummary();
  }, [tab, date, page]);

  const handleFeedback = async (payload) => {
    try {
      await submitFeedback(payload);
    } catch (e) {
      console.error("Feedback error:", e);
    }
  };

  const totalPages = Math.ceil(total / PAGE_SIZE);

  const parseTopFeatures = (str) => {
    try { return JSON.parse(str); } catch { return []; }
  };

  return (
    <div style={s.page}>
      <div style={s.header}>
        <h1 style={s.title}>Live Inferencing</h1>
        <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
          <label style={{ color: "#94a3b8", fontSize: 13 }}>Date:</label>
          <input type="date" value={date} onChange={(e) => { setDate(e.target.value); setPage(1); }}
            style={s.input} />
        </div>
        {total > 0 && <span style={{ color: "#64748b", fontSize: 13 }}>{total} breaks</span>}
      </div>

      <div style={s.tabs}>
        {[["breaks", "Today's Breaks"], ["summary", "Summary"]].map(([key, label]) => (
          <button key={key} style={s.tab(tab === key)} onClick={() => setTab(key)}>{label}</button>
        ))}
      </div>

      {error && <div style={{ color: "#f87171", background: "#1e293b", borderRadius: 6, padding: 12, marginBottom: 16 }}>{error}</div>}
      {loading && <div style={{ color: "#94a3b8", padding: 20 }}>Loading…</div>}

      {tab === "breaks" && !loading && (
        <>
          <div style={{ overflowX: "auto" }}>
            <table style={s.table}>
              <thead>
                <tr>
                  {["Trade Ref", "Rec", "Asset", "GBP", "Age", "Bucket", "Prediction", "Confidence", "Top Drivers", "Recurring", "SLA Breach", "Priority", "Feedback"].map((h) => (
                    <th key={h} style={s.th}>{h}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {rows.map((r, i) => (
                  <tr key={i} style={{ background: i % 2 === 0 ? "#0f172a" : "transparent" }}>
                    <td style={s.td}><code style={{ fontSize: 12 }}>{r.trade_ref}</code></td>
                    <td style={s.td}>{r.rec_id || "—"}</td>
                    <td style={s.td}>{r.asset_class || "—"}</td>
                    <td style={s.td}>£{r.abs_gbp != null ? Number(r.abs_gbp).toLocaleString() : "—"}</td>
                    <td style={s.td}>{r.age_days != null ? `${r.age_days}d` : "—"}</td>
                    <td style={s.td}>{r.age_bucket || "—"}</td>
                    <td style={s.td}><PredBadge pred={r.ml_rag_prediction} /></td>
                    <td style={s.td}><ConfBar value={r.ml_confidence} /></td>
                    <td style={s.td}>
                      <div style={{ fontSize: 11, color: "#94a3b8" }}>
                        {parseTopFeatures(r.ml_top_features).map((f, fi) => (
                          <div key={fi}>{f}</div>
                        ))}
                      </div>
                    </td>
                    <td style={s.td}>
                      {r.recurring_break_flag ? <span style={{ color: "#f59e0b", fontSize: 12 }}>Yes</span> : <span style={{ color: "#475569", fontSize: 12 }}>No</span>}
                    </td>
                    <td style={s.td}>
                      {r.sla_breach ? <span style={{ color: "#ef4444", fontSize: 12 }}>Breached</span> : <span style={{ color: "#22c55e", fontSize: 12 }}>OK</span>}
                    </td>
                    <td style={s.td}>{r.jira_priority ? <span style={{ color: "#f59e0b", fontWeight: 600, fontSize: 12 }}>{r.jira_priority}</span> : "—"}</td>
                    <td style={s.td}><FeedbackRow row={r} onFeedback={handleFeedback} /></td>
                  </tr>
                ))}
                {rows.length === 0 && (
                  <tr><td colSpan={13} style={{ ...s.td, textAlign: "center", color: "#475569", padding: 32 }}>No breaks found for {date}</td></tr>
                )}
              </tbody>
            </table>
          </div>

          {totalPages > 1 && (
            <div style={s.pagination}>
              <button style={s.btn} disabled={page <= 1} onClick={() => setPage((p) => p - 1)}>← Prev</button>
              <span style={{ color: "#94a3b8", fontSize: 13 }}>Page {page} / {totalPages}</span>
              <button style={s.btn} disabled={page >= totalPages} onClick={() => setPage((p) => p + 1)}>Next →</button>
            </div>
          )}
        </>
      )}

      {tab === "summary" && !loading && summary && (
        <div style={{ display: "flex", flexDirection: "column", gap: 20 }}>
          {/* KPIs */}
          <div style={s.grid}>
            <div style={s.kpi}><div style={s.kpiLabel}>Total Breaks</div><div style={s.kpiValue}>{summary.confidence_stats?.total_breaks ?? "—"}</div></div>
            <div style={s.kpi}><div style={s.kpiLabel}>Avg Confidence</div><div style={s.kpiValue}>{summary.confidence_stats?.avg_confidence != null ? `${Math.round(summary.confidence_stats.avg_confidence * 100)}%` : "—"}</div></div>
            <div style={s.kpi}><div style={s.kpiLabel}>New Themes</div><div style={s.kpiValue}>{summary.new_theme_count ?? 0}</div></div>
            <div style={s.kpi}><div style={s.kpiLabel}>No Confidence</div><div style={s.kpiValue}>{summary.confidence_stats?.no_confidence_count ?? 0}</div></div>
          </div>

          {/* Prediction Distribution Chart */}
          <div style={s.card}>
            <div style={{ fontWeight: 600, marginBottom: 12, color: "#f1f5f9" }}>Prediction Distribution</div>
            <ResponsiveContainer width="100%" height={200}>
              <BarChart data={summary.prediction_distribution || []}>
                <XAxis dataKey="prediction" stroke="#475569" tick={{ fill: "#94a3b8", fontSize: 12 }} />
                <YAxis stroke="#475569" tick={{ fill: "#94a3b8", fontSize: 12 }} />
                <Tooltip contentStyle={{ background: "#1e293b", border: "1px solid #334155", color: "#cbd5e1" }} />
                <Bar dataKey="count" radius={[4, 4, 0, 0]}>
                  {(summary.prediction_distribution || []).map((entry, i) => (
                    <Cell key={i} fill={PRED_COLORS[entry.prediction] || "#64748b"} />
                  ))}
                </Bar>
              </BarChart>
            </ResponsiveContainer>
          </div>

          {/* Top Feature Sets */}
          {summary.top_feature_sets?.length > 0 && (
            <div style={s.card}>
              <div style={{ fontWeight: 600, marginBottom: 12, color: "#f1f5f9" }}>Common Feature Patterns</div>
              <table style={s.table}>
                <thead><tr><th style={s.th}>Top Features</th><th style={s.th}>Break Count</th></tr></thead>
                <tbody>
                  {summary.top_feature_sets.map((r, i) => (
                    <tr key={i}>
                      <td style={s.td}><code style={{ fontSize: 12, color: "#7dd3fc" }}>{r.ml_top_features}</code></td>
                      <td style={s.td}>{r.cnt}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </div>
      )}
    </div>
  );
}
