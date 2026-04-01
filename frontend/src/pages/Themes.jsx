import React, { useEffect, useState } from "react";
import {
  BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer,
  LineChart, Line, CartesianGrid, Legend,
} from "recharts";
import { getThemeSummary, getThemeTrend } from "../api.js";
import Section from "../components/Section.jsx";

const fmt = (n) => n == null ? "—" : Number(n).toLocaleString("en-GB", { maximumFractionDigits: 0 });
const fmtGBP = (n) => n == null ? "—" : "£" + (Number(n) >= 1e6 ? (Number(n) / 1e6).toFixed(1) + "M" : (Number(n) / 1e3).toFixed(0) + "K");

const COLORS = [
  "#38bdf8", "#f59e0b", "#10b981", "#f43f5e", "#a78bfa",
  "#fb923c", "#34d399", "#818cf8", "#f472b6", "#facc15",
];

const s = {
  table: { width: "100%", borderCollapse: "collapse", fontSize: 12 },
  th: { textAlign: "left", padding: "8px 10px", color: "#64748b", fontWeight: 600, borderBottom: "1px solid #334155", whiteSpace: "nowrap" },
  td: { padding: "8px 10px", borderBottom: "1px solid #1e293b", color: "#e2e8f0", verticalAlign: "middle" },
};

export default function ThemesPage() {
  const [themes, setThemes] = useState([]);
  const [trend, setTrend] = useState([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    Promise.all([getThemeSummary(), getThemeTrend(30)])
      .then(([t, tr]) => { setThemes(t); setTrend(tr); })
      .catch(console.error)
      .finally(() => setLoading(false));
  }, []);

  // Pivot trend data: date -> { theme: count }
  const trendPivot = [];
  const themeSet = [...new Set(trend.map((r) => r.theme))].slice(0, 6);
  const dateSet = [...new Set(trend.map((r) => r.dt))].sort();
  for (const dt of dateSet) {
    const row = { dt: dt.slice(5) };
    for (const th of themeSet) {
      const found = trend.find((r) => r.dt === dt && r.theme === th);
      row[th] = found ? found.break_count : 0;
    }
    trendPivot.push(row);
  }

  return (
    <div>
      <h1 style={{ color: "#f1f5f9", fontSize: 20, fontWeight: 700, marginBottom: 16 }}>Themes</h1>

      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 16, marginBottom: 16 }}>
        {/* Top themes bar */}
        <Section title="Top Themes by GBP Exposure">
          <ResponsiveContainer width="100%" height={280}>
            <BarChart
              data={themes.slice(0, 10)}
              layout="vertical"
              margin={{ top: 4, right: 16, left: 140, bottom: 0 }}
            >
              <CartesianGrid strokeDasharray="3 3" stroke="#334155" horizontal={false} />
              <XAxis type="number" tickFormatter={(v) => fmtGBP(v)} tick={{ fill: "#94a3b8", fontSize: 10 }} />
              <YAxis type="category" dataKey="theme" tick={{ fill: "#94a3b8", fontSize: 11 }} width={135} />
              <Tooltip
                formatter={(v) => [fmtGBP(v), "GBP"]}
                contentStyle={{ background: "#1e293b", border: "1px solid #334155", borderRadius: 8 }}
                labelStyle={{ color: "#94a3b8" }}
              />
              <Bar dataKey="total_gbp" name="GBP" radius={[0, 4, 4, 0]}>
                {themes.slice(0, 10).map((_, i) => (
                  <rect key={i} fill={COLORS[i % COLORS.length]} />
                ))}
              </Bar>
            </BarChart>
          </ResponsiveContainer>
        </Section>

        {/* Trend */}
        <Section title="Theme Trend (last 30 days)">
          <ResponsiveContainer width="100%" height={280}>
            <LineChart data={trendPivot} margin={{ top: 4, right: 8, left: 0, bottom: 0 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#334155" />
              <XAxis dataKey="dt" tick={{ fill: "#94a3b8", fontSize: 10 }} interval="preserveStartEnd" />
              <YAxis tick={{ fill: "#94a3b8", fontSize: 11 }} />
              <Tooltip
                contentStyle={{ background: "#1e293b", border: "1px solid #334155", borderRadius: 8 }}
                labelStyle={{ color: "#94a3b8" }}
              />
              <Legend wrapperStyle={{ fontSize: 11 }} />
              {themeSet.map((th, i) => (
                <Line key={th} type="monotone" dataKey={th} stroke={COLORS[i % COLORS.length]}
                  dot={false} strokeWidth={2} />
              ))}
            </LineChart>
          </ResponsiveContainer>
        </Section>
      </div>

      {/* Theme table */}
      <Section title="All Themes">
        {loading ? (
          <div style={{ color: "#64748b", textAlign: "center", padding: 40 }}>Loading…</div>
        ) : (
          <div style={{ overflowX: "auto" }}>
            <table style={s.table}>
              <thead>
                <tr>
                  {["Theme", "Break Count", "GBP Exposure", "Recs Affected", "Platforms", "Avg Risk Score"].map((h) => (
                    <th key={h} style={s.th}>{h}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {themes.map((r, i) => (
                  <tr key={r.theme}
                    onMouseEnter={(e) => e.currentTarget.style.background = "#243044"}
                    onMouseLeave={(e) => e.currentTarget.style.background = ""}>
                    <td style={{ ...s.td, display: "flex", alignItems: "center", gap: 8 }}>
                      <span style={{
                        display: "inline-block", width: 10, height: 10, borderRadius: "50%",
                        background: COLORS[i % COLORS.length], flexShrink: 0,
                      }} />
                      {r.theme}
                    </td>
                    <td style={{ ...s.td, textAlign: "right" }}>{fmt(r.break_count)}</td>
                    <td style={{ ...s.td, textAlign: "right" }}>{fmtGBP(r.total_gbp)}</td>
                    <td style={{ ...s.td, textAlign: "right" }}>{fmt(r.rec_count)}</td>
                    <td style={{ ...s.td, textAlign: "right" }}>{fmt(r.platform_count)}</td>
                    <td style={{ ...s.td, textAlign: "right" }}>
                      <span style={{ color: riskColor(r.avg_risk_score) }}>
                        {r.avg_risk_score != null ? r.avg_risk_score.toFixed(3) : "—"}
                      </span>
                    </td>
                  </tr>
                ))}
                {themes.length === 0 && (
                  <tr><td colSpan={6} style={{ ...s.td, textAlign: "center", color: "#64748b" }}>No theme data</td></tr>
                )}
              </tbody>
            </table>
          </div>
        )}
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
