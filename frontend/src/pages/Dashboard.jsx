import React, { useEffect, useState, useCallback } from "react";
import {
  BarChart, Bar, LineChart, Line, XAxis, YAxis, CartesianGrid,
  Tooltip, Legend, ResponsiveContainer,
} from "recharts";
import {
  getKpis, getAgeProfile, getProductBreakdown, getTrend, getValidationSummary,
} from "../api.js";
import KPICard from "../components/KPICard.jsx";
import Section from "../components/Section.jsx";

const PRODUCTS = ["CEQ", "OTC", "LnD", "MTN"];
const LINE_COLORS = ["#38bdf8", "#a78bfa", "#34d399", "#fb923c"];

const s = {
  page: { color: "#f1f5f9" },
  filterBar: {
    display: "flex",
    flexWrap: "wrap",
    gap: 10,
    marginBottom: 20,
    alignItems: "center",
  },
  filterLabel: { color: "#94a3b8", fontSize: 13 },
  input: {
    background: "#1e293b",
    border: "1px solid #334155",
    borderRadius: 6,
    color: "#f1f5f9",
    padding: "5px 10px",
    fontSize: 13,
    outline: "none",
  },
  chip: (active) => ({
    padding: "4px 12px",
    borderRadius: 20,
    fontSize: 12,
    fontWeight: 600,
    cursor: "pointer",
    border: "1px solid",
    borderColor: active ? "#38bdf8" : "#334155",
    background: active ? "rgba(56,189,248,0.15)" : "transparent",
    color: active ? "#38bdf8" : "#94a3b8",
    transition: "all 0.15s",
  }),
  kpiRow: { display: "flex", flexWrap: "wrap", gap: 14, marginBottom: 24 },
  chartGrid: { display: "grid", gridTemplateColumns: "1fr 1fr", gap: 20 },
  chartBox: {
    background: "#1e293b",
    border: "1px solid #334155",
    borderRadius: 10,
    padding: "16px 12px",
  },
  chartTitle: { color: "#94a3b8", fontSize: 13, fontWeight: 600, marginBottom: 12 },
  err: { color: "#f87171", fontSize: 13 },
};

function useFilters() {
  const [products, setProducts] = useState([]);
  const [recId, setRecId] = useState("");
  const [sourceSystem, setSourceSystem] = useState("");
  const [dateFrom, setDateFrom] = useState("");
  const [dateTo, setDateTo] = useState("");

  function toggleProduct(p) {
    setProducts((prev) =>
      prev.includes(p) ? prev.filter((x) => x !== p) : [...prev, p]
    );
  }

  const params = {
    ...(products.length ? { product: products } : {}),
    ...(recId ? { rec_id: recId } : {}),
    ...(sourceSystem ? { source_system: sourceSystem } : {}),
    ...(dateFrom ? { date_from: dateFrom } : {}),
    ...(dateTo ? { date_to: dateTo } : {}),
  };

  return { products, toggleProduct, recId, setRecId, sourceSystem, setSourceSystem, dateFrom, setDateFrom, dateTo, setDateTo, params };
}

export default function Dashboard() {
  const { products, toggleProduct, recId, setRecId, sourceSystem, setSourceSystem,
          dateFrom, setDateFrom, dateTo, setDateTo, params } = useFilters();

  const [kpis, setKpis] = useState(null);
  const [ageProfile, setAgeProfile] = useState([]);
  const [prodBreakdown, setProdBreakdown] = useState([]);
  const [trend, setTrend] = useState([]);
  const [valSummary, setValSummary] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  const load = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const [k, a, p, t, v] = await Promise.all([
        getKpis(params),
        getAgeProfile(params),
        getProductBreakdown(params),
        getTrend(params),
        getValidationSummary(),
      ]);
      setKpis(k);
      setAgeProfile(a);
      setProdBreakdown(p);
      setTrend(buildTrendSeries(t));
      setValSummary(v);
    } catch (e) {
      setError(e.message);
    } finally {
      setLoading(false);
    }
  }, [JSON.stringify(params)]);

  useEffect(() => { load(); }, [load]);

  return (
    <div style={s.page}>
      {/* Filter bar */}
      <div style={s.filterBar}>
        <span style={s.filterLabel}>Product:</span>
        {PRODUCTS.map((p) => (
          <button key={p} style={s.chip(products.includes(p))} onClick={() => toggleProduct(p)}>
            {p}
          </button>
        ))}
        <input
          style={s.input} placeholder="Rec ID" value={recId}
          onChange={(e) => setRecId(e.target.value)}
        />
        <input
          style={s.input} placeholder="Source System" value={sourceSystem}
          onChange={(e) => setSourceSystem(e.target.value)}
        />
        <input style={s.input} type="date" value={dateFrom} onChange={(e) => setDateFrom(e.target.value)} />
        <span style={{ color: "#475569" }}>→</span>
        <input style={s.input} type="date" value={dateTo} onChange={(e) => setDateTo(e.target.value)} />
        <button
          style={{ ...s.chip(false), borderColor: "#0ea5e9", color: "#0ea5e9" }}
          onClick={load}
        >
          Refresh
        </button>
      </div>

      {error && <p style={s.err}>{error}</p>}

      {/* KPI cards */}
      <div style={s.kpiRow}>
        {kpis ? (
          <>
            <KPICard label="Total Breaks"    value={kpis.total_breaks}       color="#38bdf8" />
            <KPICard label="Stale (>7d)"     value={kpis.stale_count}        color="#fb923c" />
            <KPICard label="Recurring"       value={kpis.recurring_count}    color="#a78bfa" />
            <KPICard label="CCY Mismatch"    value={kpis.ccy_mismatch_count} color="#f59e0b" />
            <KPICard label="Errors"          value={kpis.error_count}        color="#f87171" />
            <KPICard label="Warnings"        value={kpis.warning_count}      color="#fbbf24" />
            <KPICard label="Files Loaded"    value={kpis.files_loaded}       color="#34d399" />
          </>
        ) : (
          !loading && <p style={s.err}>No KPI data</p>
        )}
      </div>

      {/* Charts */}
      <div style={s.chartGrid}>
        {/* Age Profile */}
        <div style={s.chartBox}>
          <div style={s.chartTitle}>Age Profile</div>
          <ResponsiveContainer width="100%" height={220}>
            <BarChart data={ageProfile} margin={{ top: 5, right: 10, bottom: 5, left: 0 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#334155" />
              <XAxis dataKey="age_bucket" tick={{ fill: "#94a3b8", fontSize: 12 }} />
              <YAxis tick={{ fill: "#94a3b8", fontSize: 12 }} />
              <Tooltip contentStyle={{ background: "#1e293b", border: "1px solid #334155", color: "#f1f5f9" }} />
              <Bar dataKey="count" fill="#38bdf8" radius={[4, 4, 0, 0]} />
            </BarChart>
          </ResponsiveContainer>
        </div>

        {/* Product Breakdown */}
        <div style={s.chartBox}>
          <div style={s.chartTitle}>Product Breakdown</div>
          <ResponsiveContainer width="100%" height={220}>
            <BarChart data={prodBreakdown} margin={{ top: 5, right: 10, bottom: 5, left: 0 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#334155" />
              <XAxis dataKey="product" tick={{ fill: "#94a3b8", fontSize: 12 }} />
              <YAxis tick={{ fill: "#94a3b8", fontSize: 12 }} />
              <Tooltip contentStyle={{ background: "#1e293b", border: "1px solid #334155", color: "#f1f5f9" }} />
              <Legend wrapperStyle={{ fontSize: 12, color: "#94a3b8" }} />
              <Bar dataKey="count"           name="Total"     fill="#38bdf8" radius={[3, 3, 0, 0]} />
              <Bar dataKey="stale_count"     name="Stale"     fill="#fb923c" radius={[3, 3, 0, 0]} />
              <Bar dataKey="recurring_count" name="Recurring" fill="#a78bfa" radius={[3, 3, 0, 0]} />
            </BarChart>
          </ResponsiveContainer>
        </div>

        {/* Daily Trend */}
        <div style={s.chartBox}>
          <div style={s.chartTitle}>Daily Trend by Product</div>
          <ResponsiveContainer width="100%" height={220}>
            <LineChart data={trend} margin={{ top: 5, right: 10, bottom: 5, left: 0 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#334155" />
              <XAxis dataKey="file_date" tick={{ fill: "#94a3b8", fontSize: 11 }} />
              <YAxis tick={{ fill: "#94a3b8", fontSize: 12 }} />
              <Tooltip contentStyle={{ background: "#1e293b", border: "1px solid #334155", color: "#f1f5f9" }} />
              <Legend wrapperStyle={{ fontSize: 12, color: "#94a3b8" }} />
              {PRODUCTS.map((p, i) => (
                <Line key={p} type="monotone" dataKey={p} stroke={LINE_COLORS[i]} dot={false} strokeWidth={2} />
              ))}
            </LineChart>
          </ResponsiveContainer>
        </div>

        {/* Validation Summary */}
        <div style={s.chartBox}>
          <div style={s.chartTitle}>Validation Issues by Type</div>
          <ResponsiveContainer width="100%" height={220}>
            <BarChart data={valSummary} layout="vertical" margin={{ top: 5, right: 10, bottom: 5, left: 80 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#334155" />
              <XAxis type="number" tick={{ fill: "#94a3b8", fontSize: 12 }} />
              <YAxis dataKey="error_type" type="category" tick={{ fill: "#94a3b8", fontSize: 11 }} width={80} />
              <Tooltip contentStyle={{ background: "#1e293b", border: "1px solid #334155", color: "#f1f5f9" }} />
              <Bar dataKey="count" fill="#f59e0b" radius={[0, 4, 4, 0]} />
            </BarChart>
          </ResponsiveContainer>
        </div>
      </div>
    </div>
  );
}

/** Pivot trend rows [{file_date, product, count}] into [{file_date, CEQ, OTC, ...}] */
function buildTrendSeries(rows) {
  const map = {};
  for (const r of rows) {
    if (!map[r.file_date]) map[r.file_date] = { file_date: r.file_date };
    map[r.file_date][r.product] = r.count;
  }
  return Object.values(map).sort((a, b) => a.file_date.localeCompare(b.file_date));
}
