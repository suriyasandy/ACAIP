import React, { useEffect, useState, useCallback } from "react";
import {
  BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer,
  LineChart, Line, PieChart, Pie, Cell, Legend, CartesianGrid,
} from "recharts";
import {
  getSummary, getPlatformBreakdown, getAgeProfile,
  getResolutionTrend, getAssetClassBreakdown,
} from "../api.js";
import KPICard from "../components/KPICard.jsx";
import Section from "../components/Section.jsx";

const COLORS = ["#38bdf8", "#f59e0b", "#10b981", "#f43f5e", "#a78bfa", "#fb923c"];

const fmt = (n) => n == null ? "—" : Number(n).toLocaleString("en-GB", { maximumFractionDigits: 0 });
const fmtGBP = (n) => n == null ? "—" : "£" + (n >= 1e6 ? (n / 1e6).toFixed(1) + "M" : (n / 1e3).toFixed(0) + "K");
const fmtPct = (n) => n == null ? "—" : Number(n).toFixed(1) + "%";

const FILTER_S = {
  bar: { display: "flex", gap: 10, marginBottom: 20, flexWrap: "wrap" },
  label: { color: "#94a3b8", fontSize: 12, display: "flex", flexDirection: "column", gap: 4 },
  select: {
    background: "#0f172a", border: "1px solid #334155", borderRadius: 6,
    color: "#e2e8f0", padding: "5px 10px", fontSize: 12, cursor: "pointer",
  },
  btn: (active) => ({
    background: active ? "#0ea5e9" : "#0f172a",
    border: "1px solid #334155", borderRadius: 6,
    color: active ? "#fff" : "#94a3b8", padding: "5px 12px", fontSize: 12,
    cursor: "pointer", transition: "background 0.15s",
  }),
};

function Filters({ filters, onChange }) {
  const toggle = (key, val) => {
    const arr = filters[key] || [];
    const next = arr.includes(val) ? arr.filter((x) => x !== val) : [...arr, val];
    onChange({ ...filters, [key]: next.length ? next : undefined });
  };
  const platforms = ["TLM", "DUCO", "D3S"];
  const assets = ["CEQ", "LnD", "MTN", "OTC"];
  return (
    <div style={FILTER_S.bar}>
      <span style={{ color: "#94a3b8", fontSize: 12, alignSelf: "center" }}>Platform:</span>
      {platforms.map((p) => (
        <button key={p} style={FILTER_S.btn(filters.platform?.includes(p))}
          onClick={() => toggle("platform", p)}>{p}</button>
      ))}
      <span style={{ color: "#334155", alignSelf: "center" }}>|</span>
      <span style={{ color: "#94a3b8", fontSize: 12, alignSelf: "center" }}>Asset:</span>
      {assets.map((a) => (
        <button key={a} style={FILTER_S.btn(filters.asset_class?.includes(a))}
          onClick={() => toggle("asset_class", a)}>{a}</button>
      ))}
    </div>
  );
}

export default function Dashboard() {
  const [filters, setFilters] = useState({});
  const [summary, setSummary] = useState(null);
  const [platform, setPlatform] = useState([]);
  const [age, setAge] = useState([]);
  const [trend, setTrend] = useState([]);
  const [assetClass, setAssetClass] = useState([]);
  const [loading, setLoading] = useState(true);

  const load = useCallback(async (f) => {
    setLoading(true);
    try {
      const [s, p, a, t, ac] = await Promise.all([
        getSummary(f),
        getPlatformBreakdown(f),
        getAgeProfile(f),
        getResolutionTrend(30),
        getAssetClassBreakdown(),
      ]);
      setSummary(s);
      setPlatform(p);
      setAge(a);
      setTrend(t);
      setAssetClass(ac);
    } catch (e) {
      console.error(e);
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => { load(filters); }, [filters, load]);

  const acHeatmap = assetClass.reduce((acc, row) => {
    if (!acc[row.asset_class]) acc[row.asset_class] = {};
    acc[row.asset_class][row.platform] = row;
    return acc;
  }, {});

  const acList = [...new Set(assetClass.map((r) => r.asset_class))];
  const platformList = [...new Set(assetClass.map((r) => r.platform))];

  return (
    <div>
      <h1 style={{ color: "#f1f5f9", fontSize: 20, fontWeight: 700, marginBottom: 16 }}>
        Break Management Dashboard
      </h1>

      <Filters filters={filters} onChange={setFilters} />

      {/* KPI row */}
      <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fill, minmax(180px, 1fr))", gap: 12, marginBottom: 24 }}>
        <KPICard label="Total Breaks"       value={fmt(summary?.total_breaks)}       sub="all time in view" />
        <KPICard label="GBP Exposure"       value={fmtGBP(summary?.total_gbp_exposure)} accent="#f59e0b" sub="absolute value" />
        <KPICard label="Open Breaks"        value={fmt(summary?.open_breaks)}         accent="#f43f5e" />
        <KPICard label="Material Breaks"    value={fmt(summary?.material_breaks)}     accent="#fb923c" sub="above threshold" />
        <KPICard label="EMIR Flagged"       value={fmt(summary?.emir_flagged)}        accent="#a78bfa" sub="OTC regulatory" />
        <KPICard label="Escalated"          value={fmt(summary?.escalated)}           accent="#f43f5e" />
        <KPICard label="Jira Coverage"      value={fmtPct(summary?.jira_coverage_pct)} accent="#10b981" sub="auto-tagged" />
        <KPICard label="Avg Age"            value={summary?.avg_age_days != null ? summary.avg_age_days + "d" : "—"} />
      </div>

      {/* Charts row 1 */}
      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 16, marginBottom: 16 }}>
        <Section title="Platform Breakdown – GBP Exposure">
          <ResponsiveContainer width="100%" height={220}>
            <BarChart data={platform} margin={{ top: 4, right: 8, left: 0, bottom: 0 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#334155" />
              <XAxis dataKey="platform" tick={{ fill: "#94a3b8", fontSize: 12 }} />
              <YAxis tickFormatter={(v) => fmtGBP(v)} tick={{ fill: "#94a3b8", fontSize: 11 }} width={68} />
              <Tooltip
                formatter={(v, n) => [fmtGBP(v), "GBP"]}
                contentStyle={{ background: "#1e293b", border: "1px solid #334155", borderRadius: 8 }}
                labelStyle={{ color: "#94a3b8" }}
              />
              <Bar dataKey="total_gbp" name="GBP Exposure" radius={[4, 4, 0, 0]}>
                {platform.map((_, i) => <Cell key={i} fill={COLORS[i % COLORS.length]} />)}
              </Bar>
            </BarChart>
          </ResponsiveContainer>
        </Section>

        <Section title="Age Profile">
          <ResponsiveContainer width="100%" height={220}>
            <BarChart data={age} margin={{ top: 4, right: 8, left: 0, bottom: 0 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#334155" />
              <XAxis dataKey="age_bucket" tick={{ fill: "#94a3b8", fontSize: 12 }} />
              <YAxis tick={{ fill: "#94a3b8", fontSize: 11 }} />
              <Tooltip
                contentStyle={{ background: "#1e293b", border: "1px solid #334155", borderRadius: 8 }}
                labelStyle={{ color: "#94a3b8" }}
              />
              <Bar dataKey="break_count" name="Breaks" fill="#38bdf8" radius={[4, 4, 0, 0]} />
              <Bar dataKey="total_gbp"   name="GBP" fill="#f59e0b" radius={[4, 4, 0, 0]} yAxisId="right" hide />
            </BarChart>
          </ResponsiveContainer>
        </Section>
      </div>

      {/* Charts row 2 */}
      <div style={{ display: "grid", gridTemplateColumns: "2fr 1fr", gap: 16, marginBottom: 16 }}>
        <Section title="Resolution Trend (last 30 days)">
          <ResponsiveContainer width="100%" height={220}>
            <LineChart data={trend} margin={{ top: 4, right: 8, left: 0, bottom: 0 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#334155" />
              <XAxis dataKey="dt" tick={{ fill: "#94a3b8", fontSize: 10 }}
                tickFormatter={(v) => v ? v.slice(5) : ""} interval="preserveStartEnd" />
              <YAxis tick={{ fill: "#94a3b8", fontSize: 11 }} />
              <Tooltip
                contentStyle={{ background: "#1e293b", border: "1px solid #334155", borderRadius: 8 }}
                labelStyle={{ color: "#94a3b8" }}
              />
              <Legend wrapperStyle={{ fontSize: 12 }} />
              <Line type="monotone" dataKey="open_count"     name="Open"     stroke="#f43f5e" dot={false} strokeWidth={2} />
              <Line type="monotone" dataKey="closed_count"   name="Closed"   stroke="#10b981" dot={false} strokeWidth={2} />
              <Line type="monotone" dataKey="resolved_count" name="Resolved" stroke="#38bdf8" dot={false} strokeWidth={2} />
            </LineChart>
          </ResponsiveContainer>
        </Section>

        <Section title="Platform Mix">
          <ResponsiveContainer width="100%" height={220}>
            <PieChart>
              <Pie
                data={platform}
                dataKey="break_count"
                nameKey="platform"
                cx="50%" cy="50%"
                innerRadius={55}
                outerRadius={85}
                paddingAngle={3}
                label={({ platform: p, percent }) => `${p} ${(percent * 100).toFixed(0)}%`}
                labelLine={false}
              >
                {platform.map((_, i) => <Cell key={i} fill={COLORS[i % COLORS.length]} />)}
              </Pie>
              <Tooltip
                formatter={(v) => [fmt(v), "breaks"]}
                contentStyle={{ background: "#1e293b", border: "1px solid #334155", borderRadius: 8 }}
              />
            </PieChart>
          </ResponsiveContainer>
        </Section>
      </div>

      {/* Asset class heatmap */}
      <Section title="Asset Class × Platform Heatmap">
        <div style={{ overflowX: "auto" }}>
          <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 13 }}>
            <thead>
              <tr>
                <th style={th}>Asset Class</th>
                {platformList.map((p) => (
                  <th key={p} style={th}>{p}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {acList.map((ac) => (
                <tr key={ac}>
                  <td style={{ ...td, color: "#94a3b8", fontWeight: 600 }}>{ac}</td>
                  {platformList.map((p) => {
                    const cell = acHeatmap[ac]?.[p];
                    return (
                      <td key={p} style={{ ...td, background: cell ? heatColor(cell.total_gbp, 5e6) : "transparent" }}>
                        {cell ? (
                          <div>
                            <div style={{ color: "#f1f5f9", fontWeight: 600 }}>{fmtGBP(cell.total_gbp)}</div>
                            <div style={{ color: "#94a3b8", fontSize: 11 }}>{fmt(cell.break_count)} breaks</div>
                          </div>
                        ) : <span style={{ color: "#334155" }}>—</span>}
                      </td>
                    );
                  })}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </Section>

      {loading && (
        <div style={{ position: "fixed", bottom: 20, right: 24, color: "#94a3b8", fontSize: 12 }}>
          Loading...
        </div>
      )}
    </div>
  );
}

const th = {
  textAlign: "left", padding: "8px 12px", color: "#94a3b8",
  fontSize: 12, fontWeight: 600, borderBottom: "1px solid #334155",
};
const td = {
  padding: "10px 12px", borderBottom: "1px solid #1e293b", transition: "background 0.15s",
};

function heatColor(val, max) {
  const pct = Math.min(val / max, 1);
  if (pct > 0.7) return "rgba(244,63,94,0.25)";
  if (pct > 0.4) return "rgba(245,158,11,0.2)";
  if (pct > 0.1) return "rgba(56,189,248,0.15)";
  return "rgba(56,189,248,0.05)";
}
