import React, { useState, useEffect, useCallback } from "react";
import {
  BarChart, Bar, PieChart, Pie, Cell,
  XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer,
} from "recharts";
import {
  getSummary, getAgeProfile, getAssetClassBreakdown,
  getRagBreakdown, getTrueSystemicBreakdown, getTeamBreakdown, getThemeSummary,
} from "../api.js";
import KPICard from "../components/KPICard.jsx";
import Section from "../components/Section.jsx";

const PLATFORMS     = ["TLM", "DUCO", "D3S"];
const ASSET_CLASSES = ["CEQ", "LnD", "MTN", "OTC"];

const RAG_COLOR  = { R: "#f43f5e", A: "#f59e0b", G: "#10b981", UNKNOWN: "#475569" };
const RAG_LABEL  = { R: "Red", A: "Amber", G: "Green", UNKNOWN: "Unknown" };
const ASSET_COLORS = { CEQ: "#38bdf8", LnD: "#34d399", MTN: "#fbbf24", OTC: "#a78bfa" };
const TS_COLORS  = ["#38bdf8", "#f59e0b", "#10b981", "#f43f5e", "#a78bfa", "#64748b"];

const fmt = (v) => {
  if (v == null) return "—";
  const n = Number(v);
  if (isNaN(n)) return String(v);
  if (n >= 1_000_000) return `£${(n / 1_000_000).toFixed(1)}M`;
  if (n >= 1_000)     return `£${(n / 1_000).toFixed(0)}K`;
  return `£${n.toFixed(0)}`;
};
const fmtN = (v) => (v == null ? "—" : Number(v).toLocaleString());

const styles = {
  page:   { color: "#f1f5f9" },
  row2:   { display: "grid", gridTemplateColumns: "2fr 1fr", gap: 20, marginBottom: 20 },
  row2eq: { display: "grid", gridTemplateColumns: "1fr 1fr", gap: 20, marginBottom: 20 },
  kpiRow: { display: "grid", gridTemplateColumns: "repeat(6, 1fr)", gap: 14, marginBottom: 20 },
  filterBar: {
    display: "flex", gap: 8, flexWrap: "wrap", alignItems: "center",
    marginBottom: 18, padding: "10px 14px",
    background: "#1e293b", borderRadius: 8,
  },
  chip: (active) => ({
    padding: "4px 12px", borderRadius: 20, fontSize: 11, fontWeight: 600,
    cursor: "pointer", border: "none",
    background: active ? "#0ea5e9" : "#334155",
    color: active ? "#fff" : "#94a3b8",
  }),
  emptyMsg: { color: "#475569", textAlign: "center", padding: "28px 0", fontSize: 13 },
  th: { textAlign: "left", padding: "7px 10px", color: "#64748b", fontWeight: 600, borderBottom: "1px solid #334155" },
  thC: { textAlign: "center", padding: "7px 10px", color: "#64748b", fontWeight: 600, borderBottom: "1px solid #334155" },
};

const TOOLTIP_STYLE = { contentStyle: { background: "#1e293b", border: "1px solid #334155", fontSize: 11 } };

function FilterBar({ platform, setPlatform, assetClass, setAssetClass }) {
  const toggle = (list, set, val) =>
    set((prev) => prev.includes(val) ? prev.filter((v) => v !== val) : [...prev, val]);
  return (
    <div style={styles.filterBar}>
      <span style={{ color: "#64748b", fontSize: 11, marginRight: 4 }}>Platform:</span>
      {PLATFORMS.map((p) => (
        <button key={p} style={styles.chip(platform.includes(p))}
          onClick={() => toggle(platform, setPlatform, p)}>{p}</button>
      ))}
      <span style={{ color: "#334155", margin: "0 8px" }}>|</span>
      <span style={{ color: "#64748b", fontSize: 11, marginRight: 4 }}>Asset:</span>
      {ASSET_CLASSES.map((a) => (
        <button key={a} style={styles.chip(assetClass.includes(a))}
          onClick={() => toggle(assetClass, setAssetClass, a)}>{a}</button>
      ))}
      {(platform.length > 0 || assetClass.length > 0) && (
        <button style={{ ...styles.chip(false), marginLeft: 8 }}
          onClick={() => { setPlatform([]); setAssetClass([]); }}>✕ Clear</button>
      )}
    </div>
  );
}

function Empty() {
  return (
    <div style={styles.emptyMsg}>
      No data — upload a Monthly MI file to populate the dashboard.
    </div>
  );
}

export default function Dashboard() {
  const [platform,   setPlatform]   = useState([]);
  const [assetClass, setAssetClass] = useState([]);
  const [summary,    setSummary]    = useState(null);
  const [ageData,    setAgeData]    = useState([]);
  const [ragData,    setRagData]    = useState([]);
  const [tsData,     setTsData]     = useState([]);
  const [teamData,   setTeamData]   = useState([]);
  const [themeData,  setThemeData]  = useState([]);
  const [acData,     setAcData]     = useState([]);

  const load = useCallback(async () => {
    const f = {
      platform:    platform.length    ? platform    : undefined,
      asset_class: assetClass.length  ? assetClass  : undefined,
    };
    try {
      const [sum, age, rag, ts, team, theme, ac] = await Promise.all([
        getSummary(f),
        getAgeProfile(f),
        getRagBreakdown(f),
        getTrueSystemicBreakdown(f),
        getTeamBreakdown(),
        getThemeSummary(),
        getAssetClassBreakdown(),
      ]);
      setSummary(sum);
      setAgeData(Array.isArray(age)   ? age   : []);
      setRagData(Array.isArray(rag)   ? rag   : []);
      setTsData( Array.isArray(ts)    ? ts    : []);
      setTeamData(Array.isArray(team) ? team  : []);
      setThemeData(Array.isArray(theme) ? theme.slice(0, 10) : []);
      setAcData(Array.isArray(ac)     ? ac    : []);
    } catch (_) {}
  }, [platform, assetClass]);

  useEffect(() => { load(); }, [load]);

  const kpi = summary || {};

  // RAG donut
  const ragDonut = ragData.map((r) => ({
    name:  RAG_LABEL[r.rag_rating] || r.rag_rating,
    value: r.break_count,
    color: RAG_COLOR[r.rag_rating]  || RAG_COLOR.UNKNOWN,
  }));

  // True/Systemic grouped-stacked bar
  const tsCategories = [...new Set(tsData.map((r) => r.true_systemic))];
  const tsAssets     = [...new Set(tsData.map((r) => r.asset_class))].sort();
  const tsGrouped = tsAssets.map((ac) => {
    const row = { asset_class: ac };
    tsCategories.forEach((cat) => {
      const found = tsData.find((r) => r.asset_class === ac && r.true_systemic === cat);
      row[cat] = found ? found.break_count : 0;
    });
    return row;
  });

  // AC heatmap
  const heatPlatforms = [...new Set(acData.map((r) => r.platform))].sort();
  const maxGbp = Math.max(...acData.map((r) => r.total_gbp || 0), 1);
  const cellBg = (gbp) => {
    const pct = gbp / maxGbp;
    if (pct > 0.7) return "rgba(244,63,94,0.35)";
    if (pct > 0.4) return "rgba(245,158,11,0.35)";
    if (pct > 0.1) return "rgba(56,189,248,0.25)";
    return "rgba(56,189,248,0.08)";
  };

  const ragRed = ragData.find((r) => r.rag_rating === "R")?.break_count ?? 0;

  return (
    <div style={styles.page}>
      <h1 style={{ fontSize: 20, fontWeight: 700, marginBottom: 16, color: "#f1f5f9" }}>
        Dashboard
      </h1>

      <FilterBar platform={platform} setPlatform={setPlatform}
                 assetClass={assetClass} setAssetClass={setAssetClass} />

      {/* ── KPI Row ── */}
      <div style={styles.kpiRow}>
        <KPICard title="Total Breaks"    value={fmtN(kpi.total_breaks)}     sub="all loaded" />
        <KPICard title="GBP Exposure"    value={fmt(kpi.total_gbp_exposure)} sub="absolute" color="#38bdf8" />
        <KPICard title="RAG Red"         value={fmtN(ragRed)}               sub="critical"  color="#f43f5e" />
        <KPICard title="Material Breaks" value={fmtN(kpi.material_breaks)}  sub="above threshold" color="#f59e0b" />
        <KPICard title="SLA Breached"    value={fmtN(kpi.escalated)}        sub="escalated" color="#f43f5e" />
        <KPICard title="Avg Age"
          value={kpi.avg_age_days != null ? `${Number(kpi.avg_age_days).toFixed(0)}d` : "—"}
          sub="days" color="#a78bfa" />
      </div>

      {/* ── Row 1: Team GBP bar (2/3) + RAG donut (1/3) ── */}
      <div style={styles.row2}>
        <Section title="Team Breakdown – GBP Exposure">
          {teamData.length === 0 ? <Empty /> : (
            <ResponsiveContainer width="100%" height={240}>
              <BarChart data={teamData} margin={{ top: 4, right: 12, left: 0, bottom: 48 }}>
                <CartesianGrid strokeDasharray="3 3" stroke="#1e293b" />
                <XAxis dataKey="team" tick={{ fill: "#64748b", fontSize: 10 }}
                  angle={-35} textAnchor="end" interval={0} />
                <YAxis tick={{ fill: "#64748b", fontSize: 10 }}
                  tickFormatter={(v) => v >= 1e6 ? `£${(v / 1e6).toFixed(1)}M` : `£${(v / 1e3).toFixed(0)}K`} />
                <Tooltip formatter={(v) => [fmt(v), "GBP Exposure"]} {...TOOLTIP_STYLE} />
                <Bar dataKey="total_gbp" name="GBP Exposure" radius={[3, 3, 0, 0]}>
                  {teamData.map((_, i) => (
                    <Cell key={i} fill={Object.values(ASSET_COLORS)[i % 4]} />
                  ))}
                </Bar>
              </BarChart>
            </ResponsiveContainer>
          )}
        </Section>

        <Section title="RAG Distribution">
          {ragDonut.length === 0 ? <Empty /> : (
            <ResponsiveContainer width="100%" height={240}>
              <PieChart>
                <Pie data={ragDonut} dataKey="value" nameKey="name"
                  cx="50%" cy="45%" outerRadius={88} innerRadius={50}
                  label={({ name, percent }) =>
                    percent > 0.04 ? `${name} ${(percent * 100).toFixed(0)}%` : ""
                  }
                  labelLine={false}>
                  {ragDonut.map((entry, i) => (
                    <Cell key={i} fill={entry.color} />
                  ))}
                </Pie>
                <Tooltip formatter={(v, n) => [fmtN(v), n]} {...TOOLTIP_STYLE} />
                <Legend
                  formatter={(v) => <span style={{ color: "#94a3b8", fontSize: 11 }}>{v}</span>} />
              </PieChart>
            </ResponsiveContainer>
          )}
        </Section>
      </div>

      {/* ── Row 2: True vs Systemic + Top Themes ── */}
      <div style={styles.row2eq}>
        <Section title="True vs Systemic by Asset Class">
          {tsGrouped.length === 0 ? <Empty /> : (
            <ResponsiveContainer width="100%" height={240}>
              <BarChart data={tsGrouped} margin={{ top: 4, right: 12, left: 0, bottom: 4 }}>
                <CartesianGrid strokeDasharray="3 3" stroke="#1e293b" />
                <XAxis dataKey="asset_class" tick={{ fill: "#64748b", fontSize: 11 }} />
                <YAxis tick={{ fill: "#64748b", fontSize: 10 }} />
                <Tooltip {...TOOLTIP_STYLE} />
                <Legend
                  formatter={(v) => <span style={{ color: "#94a3b8", fontSize: 11 }}>{v}</span>} />
                {tsCategories.map((cat, i) => (
                  <Bar key={cat} dataKey={cat} name={cat} stackId="a"
                    fill={TS_COLORS[i % TS_COLORS.length]}
                    radius={i === tsCategories.length - 1 ? [3, 3, 0, 0] : [0, 0, 0, 0]} />
                ))}
              </BarChart>
            </ResponsiveContainer>
          )}
        </Section>

        <Section title="Top Themes by GBP Exposure">
          {themeData.length === 0 ? <Empty /> : (
            <ResponsiveContainer width="100%" height={240}>
              <BarChart data={themeData} layout="vertical"
                margin={{ top: 4, right: 40, left: 4, bottom: 4 }}>
                <CartesianGrid strokeDasharray="3 3" stroke="#1e293b" horizontal={false} />
                <XAxis type="number" tick={{ fill: "#64748b", fontSize: 9 }}
                  tickFormatter={(v) => v >= 1e6 ? `£${(v / 1e6).toFixed(1)}M` : `£${(v / 1e3).toFixed(0)}K`} />
                <YAxis type="category" dataKey="theme" width={130}
                  tick={{ fill: "#94a3b8", fontSize: 9 }} />
                <Tooltip formatter={(v) => [fmt(v), "GBP"]} {...TOOLTIP_STYLE} />
                <Bar dataKey="total_gbp" name="GBP" fill="#818cf8" radius={[0, 3, 3, 0]} />
              </BarChart>
            </ResponsiveContainer>
          )}
        </Section>
      </div>

      {/* ── Row 3: Age Profile + Asset Class Heatmap ── */}
      <div style={styles.row2eq}>
        <Section title="Age Profile (Break Count by Age Band)">
          {ageData.length === 0 ? <Empty /> : (
            <ResponsiveContainer width="100%" height={220}>
              <BarChart data={ageData} margin={{ top: 4, right: 12, left: 0, bottom: 4 }}>
                <CartesianGrid strokeDasharray="3 3" stroke="#1e293b" />
                <XAxis dataKey="age_bucket" tick={{ fill: "#64748b", fontSize: 11 }} />
                <YAxis tick={{ fill: "#64748b", fontSize: 10 }} />
                <Tooltip {...TOOLTIP_STYLE} />
                <Bar dataKey="break_count" name="Breaks" radius={[3, 3, 0, 0]}>
                  {ageData.map((_, i) => (
                    <Cell key={i} fill={["#38bdf8", "#fbbf24", "#f59e0b", "#f43f5e"][i] || "#64748b"} />
                  ))}
                </Bar>
              </BarChart>
            </ResponsiveContainer>
          )}
        </Section>

        <Section title="Asset Class × Platform Heatmap">
          {acData.length === 0 ? <Empty /> : (
            <div style={{ overflowX: "auto" }}>
              <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 11 }}>
                <thead>
                  <tr>
                    <th style={styles.th}>Asset</th>
                    {heatPlatforms.map((p) => (
                      <th key={p} style={styles.thC}>{p}</th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {ASSET_CLASSES.map((ac) => (
                    <tr key={ac}>
                      <td style={{ padding: "7px 10px", color: ASSET_COLORS[ac] || "#e2e8f0", fontWeight: 600 }}>{ac}</td>
                      {heatPlatforms.map((plat) => {
                        const cell = acData.find((r) => r.asset_class === ac && r.platform === plat);
                        return (
                          <td key={plat} style={{
                            padding: "7px 10px", textAlign: "center",
                            background: cell ? cellBg(cell.total_gbp) : "transparent",
                          }}>
                            {cell ? (
                              <>
                                <div style={{ color: "#e2e8f0", fontWeight: 600 }}>{fmt(cell.total_gbp)}</div>
                                <div style={{ color: "#64748b", fontSize: 10 }}>{fmtN(cell.break_count)} breaks</div>
                              </>
                            ) : <span style={{ color: "#334155" }}>—</span>}
                          </td>
                        );
                      })}
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </Section>
      </div>
    </div>
  );
}
