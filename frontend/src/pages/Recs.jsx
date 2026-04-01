import React, { useEffect, useState } from "react";
import { getRecs } from "../api.js";
import Section from "../components/Section.jsx";

const fmt = (n) => n == null ? "—" : Number(n).toLocaleString("en-GB", { maximumFractionDigits: 0 });
const fmtGBP = (n) => n == null ? "—" : "£" + (Number(n) >= 1e6 ? (Number(n) / 1e6).toFixed(1) + "M" : (Number(n) / 1e3).toFixed(0) + "K");
const fmtPct = (n) => n == null ? "—" : Number(n).toFixed(1) + "%";

const s = {
  table: { width: "100%", borderCollapse: "collapse", fontSize: 12 },
  th: { textAlign: "left", padding: "8px 10px", color: "#64748b", fontWeight: 600, borderBottom: "1px solid #334155", whiteSpace: "nowrap" },
  td: { padding: "8px 10px", borderBottom: "1px solid #1e293b", color: "#e2e8f0" },
};

function PctBar({ v, max = 100, color = "#38bdf8" }) {
  const pct = Math.min((v || 0) / max * 100, 100);
  return (
    <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
      <div style={{ flex: 1, height: 6, background: "#0f172a", borderRadius: 3, overflow: "hidden" }}>
        <div style={{ width: pct + "%", height: "100%", background: color, borderRadius: 3 }} />
      </div>
      <span style={{ color: "#94a3b8", fontSize: 11, minWidth: 40 }}>{fmtPct(v)}</span>
    </div>
  );
}

export default function RecsPage() {
  const [recs, setRecs] = useState([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    getRecs().then(setRecs).catch(console.error).finally(() => setLoading(false));
  }, []);

  return (
    <div>
      <h1 style={{ color: "#f1f5f9", fontSize: 20, fontWeight: 700, marginBottom: 16 }}>Reconciliations</h1>

      <Section>
        {loading ? (
          <div style={{ color: "#64748b", textAlign: "center", padding: 40 }}>Loading…</div>
        ) : (
          <div style={{ overflowX: "auto" }}>
            <table style={s.table}>
              <thead>
                <tr>
                  {["Rec ID", "Name", "Platform", "Asset Class", "Total", "Open",
                    "GBP Exposure", "Material", "Avg Age", "Jira Coverage", "SLA Days", "EMIR"].map((h) => (
                    <th key={h} style={s.th}>{h}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {recs.map((r) => (
                  <tr key={r.rec_id}
                    onMouseEnter={(e) => e.currentTarget.style.background = "#243044"}
                    onMouseLeave={(e) => e.currentTarget.style.background = ""}>
                    <td style={{ ...s.td, fontFamily: "monospace", color: "#38bdf8" }}>{r.rec_id}</td>
                    <td style={s.td}>{r.rec_name}</td>
                    <td style={{ ...s.td, color: "#94a3b8" }}>{r.source_system}</td>
                    <td style={s.td}>
                      <span style={{
                        background: acColor(r.asset_class),
                        color: "#fff", fontSize: 10, fontWeight: 700,
                        borderRadius: 3, padding: "2px 6px",
                      }}>{r.asset_class}</span>
                    </td>
                    <td style={{ ...s.td, textAlign: "right" }}>{fmt(r.total_breaks)}</td>
                    <td style={{ ...s.td, textAlign: "right", color: "#f43f5e" }}>{fmt(r.open_breaks)}</td>
                    <td style={{ ...s.td, textAlign: "right" }}>{fmtGBP(r.total_gbp)}</td>
                    <td style={{ ...s.td, textAlign: "right", color: "#f59e0b" }}>{fmt(r.material_count)}</td>
                    <td style={{ ...s.td, textAlign: "right" }}>{r.avg_age != null ? r.avg_age + "d" : "—"}</td>
                    <td style={{ ...s.td, minWidth: 140 }}>
                      <PctBar v={r.jira_pct} color={r.jira_pct >= 95 ? "#10b981" : r.jira_pct >= 70 ? "#f59e0b" : "#f43f5e"} />
                    </td>
                    <td style={{ ...s.td, textAlign: "right", color: "#94a3b8" }}>{r.escalation_sla_days ?? "—"}d</td>
                    <td style={{ ...s.td, textAlign: "center" }}>
                      {r.emir_flag ? <span style={{ color: "#a78bfa", fontWeight: 700 }}>EMIR</span> : <span style={{ color: "#334155" }}>—</span>}
                    </td>
                  </tr>
                ))}
                {recs.length === 0 && (
                  <tr><td colSpan={12} style={{ ...s.td, textAlign: "center", color: "#64748b" }}>No data</td></tr>
                )}
              </tbody>
            </table>
          </div>
        )}
      </Section>
    </div>
  );
}

function acColor(ac) {
  return { CEQ: "#0ea5e9", LnD: "#10b981", MTN: "#f59e0b", OTC: "#a78bfa" }[ac] || "#64748b";
}
