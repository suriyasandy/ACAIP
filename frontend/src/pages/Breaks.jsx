import React, { useState, useEffect, useCallback } from "react";
import { getBreaks } from "../api.js";
import Section from "../components/Section.jsx";

const fmt = (n) => n == null ? "—" : Number(n).toLocaleString("en-GB", { maximumFractionDigits: 0 });
const fmtGBP = (n) => n == null ? "—" : "£" + Number(n).toLocaleString("en-GB", { maximumFractionDigits: 0 });

const TAG_COLORS = {
  OPEN:     { background: "rgba(239,68,68,0.15)", color: "#fca5a5" },
  CLOSED:   { background: "rgba(16,185,129,0.15)", color: "#6ee7b7" },
  RESOLVED: { background: "rgba(56,189,248,0.15)", color: "#7dd3fc" },
};

function StatusBadge({ v }) {
  const style = TAG_COLORS[v] || {};
  return (
    <span style={{
      ...style, fontSize: 11, fontWeight: 600, borderRadius: 4,
      padding: "2px 6px", display: "inline-block",
    }}>
      {v || "—"}
    </span>
  );
}

function Flag({ v }) {
  if (!v) return null;
  return (
    <span style={{
      background: "rgba(244,63,94,0.15)", color: "#f43f5e",
      fontSize: 10, fontWeight: 700, borderRadius: 3, padding: "1px 5px",
    }}>
      {v}
    </span>
  );
}

const s = {
  toolbar: { display: "flex", gap: 10, marginBottom: 16, flexWrap: "wrap", alignItems: "center" },
  input: {
    background: "#0f172a", border: "1px solid #334155", borderRadius: 6,
    color: "#e2e8f0", padding: "6px 12px", fontSize: 12,
  },
  select: {
    background: "#0f172a", border: "1px solid #334155", borderRadius: 6,
    color: "#e2e8f0", padding: "6px 10px", fontSize: 12, cursor: "pointer",
  },
  btn: (active) => ({
    background: active ? "#0ea5e9" : "#0f172a",
    border: "1px solid #334155", borderRadius: 6,
    color: active ? "#fff" : "#94a3b8", padding: "6px 14px", fontSize: 12, cursor: "pointer",
  }),
  table: { width: "100%", borderCollapse: "collapse", fontSize: 12 },
  th: { textAlign: "left", padding: "8px 10px", color: "#64748b", fontWeight: 600, borderBottom: "1px solid #334155", whiteSpace: "nowrap" },
  td: { padding: "8px 10px", borderBottom: "1px solid #1e293b", color: "#e2e8f0", verticalAlign: "middle" },
  pager: { display: "flex", gap: 8, alignItems: "center", marginTop: 12, justifyContent: "flex-end" },
};

export default function Breaks() {
  const [params, setParams] = useState({ page: 1, page_size: 50 });
  const [data, setData] = useState({ total: 0, rows: [] });
  const [loading, setLoading] = useState(false);

  const set = (key, val) => setParams((p) => ({ ...p, [key]: val, page: 1 }));

  const load = useCallback(async () => {
    setLoading(true);
    try {
      const d = await getBreaks(params);
      setData(d);
    } catch (e) { console.error(e); }
    finally { setLoading(false); }
  }, [params]);

  useEffect(() => { load(); }, [load]);

  const totalPages = Math.ceil(data.total / params.page_size);

  return (
    <div>
      <h1 style={{ color: "#f1f5f9", fontSize: 20, fontWeight: 700, marginBottom: 16 }}>Breaks</h1>

      <div style={s.toolbar}>
        <select style={s.select} onChange={(e) => set("platform", e.target.value || undefined)}>
          <option value="">All Platforms</option>
          {["TLM", "DUCO", "D3S"].map((p) => <option key={p}>{p}</option>)}
        </select>
        <select style={s.select} onChange={(e) => set("asset_class", e.target.value || undefined)}>
          <option value="">All Asset Classes</option>
          {["CEQ", "LnD", "MTN", "OTC"].map((a) => <option key={a}>{a}</option>)}
        </select>
        <input style={s.input} type="date" placeholder="From"
          onChange={(e) => set("date_from", e.target.value || undefined)} />
        <input style={s.input} type="date" placeholder="To"
          onChange={(e) => set("date_to", e.target.value || undefined)} />
        <select style={s.select} onChange={(e) => set("escalation", e.target.value || undefined)}>
          <option value="">All</option>
          <option value="true">Escalated</option>
          <option value="false">Non-Escalated</option>
        </select>
        <span style={{ color: "#64748b", fontSize: 12, marginLeft: "auto" }}>
          {loading ? "Loading…" : `${fmt(data.total)} breaks`}
        </span>
      </div>

      <Section>
        <div style={{ overflowX: "auto" }}>
          <table style={s.table}>
            <thead>
              <tr>
                {["Trade Ref", "Rec", "Platform", "Asset", "Type", "Break Value", "GBP", "Age", "Bucket",
                  "Jira", "Risk", "Status", "Flags"].map((h) => (
                  <th key={h} style={s.th}>{h}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {data.rows.map((r) => (
                <tr key={r.id} style={{ transition: "background 0.1s" }}
                  onMouseEnter={(e) => e.currentTarget.style.background = "#243044"}
                  onMouseLeave={(e) => e.currentTarget.style.background = ""}>
                  <td style={{ ...s.td, fontFamily: "monospace", fontSize: 11 }}>{r.trade_ref}</td>
                  <td style={{ ...s.td, color: "#94a3b8" }}>{r.rec_id}</td>
                  <td style={s.td}>{r.source_system}</td>
                  <td style={s.td}>{r.asset_class}</td>
                  <td style={{ ...s.td, color: "#94a3b8" }}>{r.break_type}</td>
                  <td style={{ ...s.td, textAlign: "right" }}>
                    {r.break_value != null ? Number(r.break_value).toLocaleString("en-GB", { maximumFractionDigits: 0 }) : "—"}
                    {" "}<span style={{ color: "#64748b" }}>{r.break_ccy}</span>
                  </td>
                  <td style={{ ...s.td, textAlign: "right", color: r.abs_gbp > 1e6 ? "#f43f5e" : "#e2e8f0" }}>
                    {fmtGBP(r.abs_gbp)}
                  </td>
                  <td style={{ ...s.td, textAlign: "right" }}>{r.age_days}d</td>
                  <td style={{ ...s.td, color: "#94a3b8" }}>{r.age_bucket}</td>
                  <td style={{ ...s.td, fontFamily: "monospace", fontSize: 11 }}>
                    {r.jira_ref ? <span style={{ color: "#38bdf8" }}>{r.jira_ref}</span> : <span style={{ color: "#334155" }}>—</span>}
                  </td>
                  <td style={{ ...s.td, textAlign: "right" }}>
                    <span style={{ color: riskColor(r.ml_risk_score) }}>
                      {r.ml_risk_score != null ? r.ml_risk_score.toFixed(2) : "—"}
                    </span>
                  </td>
                  <td style={s.td}><StatusBadge v={r.status} /></td>
                  <td style={{ ...s.td, display: "flex", gap: 4, flexWrap: "wrap" }}>
                    {r.material_flag && <Flag v="MAT" />}
                    {r.emir_flag && <Flag v="EMIR" />}
                    {r.escalation_flag && <Flag v={r.escalation_flag} />}
                  </td>
                </tr>
              ))}
              {!loading && data.rows.length === 0 && (
                <tr><td colSpan={13} style={{ ...s.td, textAlign: "center", color: "#64748b" }}>No breaks found</td></tr>
              )}
            </tbody>
          </table>
        </div>

        <div style={s.pager}>
          <button style={s.btn(false)} disabled={params.page <= 1}
            onClick={() => setParams((p) => ({ ...p, page: p.page - 1 }))}>Prev</button>
          <span style={{ color: "#94a3b8", fontSize: 12 }}>
            Page {params.page} / {totalPages || 1}
          </span>
          <button style={s.btn(false)} disabled={params.page >= totalPages}
            onClick={() => setParams((p) => ({ ...p, page: p.page + 1 }))}>Next</button>
        </div>
      </Section>
    </div>
  );
}

function riskColor(v) {
  if (v == null) return "#64748b";
  if (v > 0.7) return "#f43f5e";
  if (v > 0.4) return "#f59e0b";
  return "#10b981";
}
