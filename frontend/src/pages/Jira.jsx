import React, { useEffect, useState } from "react";
import { getJiraCoverage, getJiraDrafts, getJiraEpics, approveDraft } from "../api.js";
import Section from "../components/Section.jsx";
import KPICard from "../components/KPICard.jsx";

const fmt = (n) => n == null ? "—" : Number(n).toLocaleString("en-GB", { maximumFractionDigits: 0 });
const fmtGBP = (n) => n == null ? "—" : "£" + Number(n).toLocaleString("en-GB", { maximumFractionDigits: 0 });
const fmtPct = (n) => n == null ? "—" : Number(n).toFixed(1) + "%";

const s = {
  table: { width: "100%", borderCollapse: "collapse", fontSize: 12 },
  th: { textAlign: "left", padding: "8px 10px", color: "#64748b", fontWeight: 600, borderBottom: "1px solid #334155" },
  td: { padding: "8px 10px", borderBottom: "1px solid #1e293b", color: "#e2e8f0", verticalAlign: "middle" },
  approveBtn: {
    background: "#10b981", color: "#fff", border: "none", borderRadius: 5,
    padding: "4px 12px", fontSize: 11, fontWeight: 600, cursor: "pointer",
  },
  tab: (active) => ({
    padding: "6px 16px", borderRadius: 6, fontSize: 13, cursor: "pointer",
    background: active ? "#0ea5e9" : "transparent",
    color: active ? "#fff" : "#94a3b8", border: "none",
  }),
};

function PctBar({ v, color }) {
  const pct = Math.min(v || 0, 100);
  return (
    <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
      <div style={{ flex: 1, height: 6, background: "#0f172a", borderRadius: 3, overflow: "hidden" }}>
        <div style={{ width: pct + "%", height: "100%", background: color || "#38bdf8", borderRadius: 3 }} />
      </div>
      <span style={{ color: "#94a3b8", fontSize: 11, minWidth: 40 }}>{fmtPct(v)}</span>
    </div>
  );
}

export default function JiraPage() {
  const [tab, setTab] = useState("coverage");
  const [coverage, setCoverage] = useState(null);
  const [drafts, setDrafts] = useState([]);
  const [epics, setEpics] = useState([]);
  const [loading, setLoading] = useState(false);
  const [approving, setApproving] = useState(null);

  useEffect(() => {
    setLoading(true);
    Promise.all([getJiraCoverage(), getJiraDrafts(), getJiraEpics()])
      .then(([c, d, e]) => { setCoverage(c); setDrafts(d); setEpics(e); })
      .catch(console.error)
      .finally(() => setLoading(false));
  }, []);

  const approve = async (ref) => {
    setApproving(ref);
    try {
      await approveDraft(ref);
      setDrafts((prev) => prev.filter((d) => d.jira_ref !== ref));
    } finally {
      setApproving(null);
    }
  };

  const overall = coverage?.overall;
  const byPlatform = coverage?.by_platform || [];
  const byAsset = coverage?.by_asset_class || [];

  return (
    <div>
      <h1 style={{ color: "#f1f5f9", fontSize: 20, fontWeight: 700, marginBottom: 16 }}>Jira</h1>

      {/* KPIs */}
      <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fill, minmax(180px, 1fr))", gap: 12, marginBottom: 20 }}>
        <KPICard label="Total Breaks"    value={fmt(overall?.total)}      />
        <KPICard label="Jira Matched"    value={fmt(overall?.matched)}    accent="#10b981" />
        <KPICard label="Coverage"        value={fmtPct(overall?.coverage_pct)} accent={
          (overall?.coverage_pct || 0) >= 95 ? "#10b981" : (overall?.coverage_pct || 0) >= 70 ? "#f59e0b" : "#f43f5e"
        } sub="target: 95%" />
        <KPICard label="Draft Tickets"   value={fmt(drafts.length)}       accent="#f59e0b" sub="awaiting approval" />
      </div>

      {/* Tabs */}
      <div style={{ display: "flex", gap: 4, marginBottom: 16 }}>
        {["coverage", "drafts", "epics"].map((t) => (
          <button key={t} style={s.tab(tab === t)} onClick={() => setTab(t)}>
            {t.charAt(0).toUpperCase() + t.slice(1)}
            {t === "drafts" && drafts.length > 0 && (
              <span style={{ marginLeft: 6, background: "#f59e0b", color: "#000", borderRadius: 10, padding: "0 5px", fontSize: 10, fontWeight: 700 }}>
                {drafts.length}
              </span>
            )}
          </button>
        ))}
      </div>

      {tab === "coverage" && (
        <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 16 }}>
          <Section title="Coverage by Platform">
            <table style={s.table}>
              <thead>
                <tr><th style={s.th}>Platform</th><th style={s.th}>Total</th><th style={s.th}>Matched</th><th style={s.th}>Coverage</th></tr>
              </thead>
              <tbody>
                {byPlatform.map((r) => (
                  <tr key={r.platform}>
                    <td style={s.td}>{r.platform}</td>
                    <td style={{ ...s.td, textAlign: "right" }}>{fmt(r.total)}</td>
                    <td style={{ ...s.td, textAlign: "right", color: "#10b981" }}>{fmt(r.matched)}</td>
                    <td style={{ ...s.td, minWidth: 160 }}>
                      <PctBar v={r.coverage_pct} color={r.coverage_pct >= 95 ? "#10b981" : r.coverage_pct >= 70 ? "#f59e0b" : "#f43f5e"} />
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </Section>

          <Section title="Coverage by Asset Class">
            <table style={s.table}>
              <thead>
                <tr><th style={s.th}>Asset</th><th style={s.th}>Total</th><th style={s.th}>Matched</th><th style={s.th}>Coverage</th></tr>
              </thead>
              <tbody>
                {byAsset.map((r) => (
                  <tr key={r.asset_class}>
                    <td style={s.td}>{r.asset_class}</td>
                    <td style={{ ...s.td, textAlign: "right" }}>{fmt(r.total)}</td>
                    <td style={{ ...s.td, textAlign: "right", color: "#10b981" }}>{fmt(r.matched)}</td>
                    <td style={{ ...s.td, minWidth: 160 }}>
                      <PctBar v={r.coverage_pct} color={r.coverage_pct >= 95 ? "#10b981" : r.coverage_pct >= 70 ? "#f59e0b" : "#f43f5e"} />
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </Section>
        </div>
      )}

      {tab === "drafts" && (
        <Section title={`Draft Tickets (${drafts.length})`}
          right={<span style={{ color: "#64748b", fontSize: 12 }}>Approve to submit to Jira</span>}>
          {drafts.length === 0 ? (
            <div style={{ color: "#64748b", textAlign: "center", padding: 32 }}>No pending drafts</div>
          ) : (
            <div style={{ overflowX: "auto" }}>
              <table style={s.table}>
                <thead>
                  <tr>
                    {["Jira Ref", "Summary", "Trade Ref", "Rec", "Asset", "GBP", "Action"].map((h) => (
                      <th key={h} style={s.th}>{h}</th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {drafts.map((d) => (
                    <tr key={d.jira_ref}
                      onMouseEnter={(e) => e.currentTarget.style.background = "#243044"}
                      onMouseLeave={(e) => e.currentTarget.style.background = ""}>
                      <td style={{ ...s.td, fontFamily: "monospace", color: "#38bdf8" }}>{d.jira_ref}</td>
                      <td style={{ ...s.td, maxWidth: 280, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{d.summary}</td>
                      <td style={{ ...s.td, fontFamily: "monospace", fontSize: 11 }}>{d.trade_ref}</td>
                      <td style={{ ...s.td, color: "#94a3b8" }}>{d.rec_id}</td>
                      <td style={s.td}>{d.asset_class}</td>
                      <td style={{ ...s.td, textAlign: "right" }}>{fmtGBP(d.break_value_gbp)}</td>
                      <td style={s.td}>
                        <button style={{ ...s.approveBtn, opacity: approving === d.jira_ref ? 0.5 : 1 }}
                          disabled={approving === d.jira_ref}
                          onClick={() => approve(d.jira_ref)}>
                          {approving === d.jira_ref ? "…" : "Approve"}
                        </button>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </Section>
      )}

      {tab === "epics" && (
        <Section title="Epic Summary">
          <div style={{ overflowX: "auto" }}>
            <table style={s.table}>
              <thead>
                <tr>
                  {["Epic", "Total", "Open", "Resolved", "Resolution Rate"].map((h) => (
                    <th key={h} style={s.th}>{h}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {epics.map((e) => (
                  <tr key={e.epic}
                    onMouseEnter={(ev) => ev.currentTarget.style.background = "#243044"}
                    onMouseLeave={(ev) => ev.currentTarget.style.background = ""}>
                    <td style={{ ...s.td, color: "#a78bfa", fontWeight: 600 }}>{e.epic}</td>
                    <td style={{ ...s.td, textAlign: "right" }}>{fmt(e.total)}</td>
                    <td style={{ ...s.td, textAlign: "right", color: "#f43f5e" }}>{fmt(e.open_count)}</td>
                    <td style={{ ...s.td, textAlign: "right", color: "#10b981" }}>{fmt(e.resolved_count)}</td>
                    <td style={{ ...s.td, minWidth: 160 }}>
                      <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
                        <div style={{ flex: 1, height: 6, background: "#0f172a", borderRadius: 3, overflow: "hidden" }}>
                          <div style={{ width: Math.min(e.coverage_pct || 0, 100) + "%", height: "100%", background: "#10b981", borderRadius: 3 }} />
                        </div>
                        <span style={{ color: "#94a3b8", fontSize: 11, minWidth: 40 }}>{fmtPct(e.coverage_pct)}</span>
                      </div>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </Section>
      )}
    </div>
  );
}
