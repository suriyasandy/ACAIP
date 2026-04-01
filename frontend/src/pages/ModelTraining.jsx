import React, { useState, useEffect } from "react";
import { Brain, CheckCircle, XCircle, Clock, RefreshCw } from "lucide-react";
import Section from "../components/Section.jsx";
import KPICard from "../components/KPICard.jsx";

const fmtPct = (n) => (n == null ? "—" : (Number(n) * 100).toFixed(1) + "%");
const fmt = (n) => (n == null ? "—" : Number(n).toLocaleString("en-GB"));
const fmtDate = (s) => (s ? new Date(s).toLocaleString() : "—");

const AC_COLOR = { CEQ: "#0ea5e9", LnD: "#10b981", MTN: "#f59e0b", OTC: "#a78bfa" };

const s = {
  card: (ac) => ({
    background: "#1e293b", border: `1px solid ${AC_COLOR[ac] || "#334155"}`,
    borderRadius: 10, padding: "16px 18px",
  }),
  acBadge: (ac) => ({
    display: "inline-block", background: AC_COLOR[ac] || "#334155",
    color: "#fff", fontSize: 11, fontWeight: 700, borderRadius: 4, padding: "2px 8px",
    marginBottom: 8,
  }),
  trainedBadge: (ok) => ({
    display: "inline-flex", alignItems: "center", gap: 4,
    background: ok ? "rgba(16,185,129,0.15)" : "rgba(100,116,139,0.15)",
    color: ok ? "#6ee7b7" : "#64748b",
    fontSize: 11, fontWeight: 600, borderRadius: 4, padding: "2px 8px",
  }),
  table: { width: "100%", borderCollapse: "collapse", fontSize: 12 },
  th: { textAlign: "left", padding: "8px 10px", color: "#64748b", fontWeight: 600, borderBottom: "1px solid #334155" },
  td: { padding: "8px 10px", borderBottom: "1px solid #1e293b", color: "#e2e8f0" },
  trainBtn: (loading) => ({
    background: loading ? "#1e3a5f" : "#0ea5e9",
    color: loading ? "#64748b" : "#fff",
    border: "none", borderRadius: 7, padding: "10px 28px", fontSize: 13, fontWeight: 600,
    cursor: loading ? "not-allowed" : "pointer", display: "flex", alignItems: "center", gap: 8,
  }),
};

function ModelCard({ m }) {
  return (
    <div style={s.card(m.asset_class)}>
      <div style={s.acBadge(m.asset_class)}>{m.asset_class}</div>
      <div style={{ marginBottom: 6 }}>
        <span style={s.trainedBadge(m.trained)}>
          {m.trained ? <CheckCircle size={11} /> : <XCircle size={11} />}
          {m.trained ? "Trained" : "Not trained"}
        </span>
      </div>
      {m.trained && (
        <>
          <div style={{ color: "#f1f5f9", fontSize: 22, fontWeight: 700, marginBottom: 2 }}>
            {fmtPct(m.last_accuracy)}
          </div>
          <div style={{ color: "#64748b", fontSize: 11 }}>accuracy</div>
          <div style={{ color: "#94a3b8", fontSize: 11, marginTop: 8 }}>
            <Clock size={10} style={{ display: "inline", marginRight: 3 }} />
            {fmtDate(m.last_trained)}
          </div>
          {m.last_n_train && (
            <div style={{ color: "#64748b", fontSize: 11 }}>{fmt(m.last_n_train)} training samples</div>
          )}
        </>
      )}
      {!m.trained && (
        <div style={{ color: "#475569", fontSize: 12, marginTop: 8 }}>
          Click "Train All Models" to train
        </div>
      )}
    </div>
  );
}

export default function ModelTraining() {
  const [models, setModels] = useState([]);
  const [training, setTraining] = useState(false);
  const [results, setResults] = useState(null);
  const [loading, setLoading] = useState(true);

  const loadModels = async () => {
    setLoading(true);
    try {
      const data = await fetch("/api/pipeline/models").then((r) => r.json());
      setModels(Array.isArray(data) ? data : []);
    } catch (e) { console.error(e); }
    finally { setLoading(false); }
  };

  useEffect(() => { loadModels(); }, []);

  const train = async () => {
    setTraining(true);
    setResults(null);
    try {
      const res = await fetch("/api/pipeline/train", { method: "POST" });
      const data = await res.json();
      setResults(data.results || []);
      await loadModels();
    } catch (e) {
      setResults([{ asset_class: "ALL", status: "ERROR", message: e.message }]);
    } finally {
      setTraining(false);
    }
  };

  const score = async () => {
    try {
      await fetch("/api/pipeline/score", { method: "POST" });
    } catch (e) { console.error(e); }
  };

  const anyTrained = models.some((m) => m.trained);
  const avgAccuracy = models.filter((m) => m.last_accuracy).reduce((s, m, _, a) => s + m.last_accuracy / a.length, 0);

  return (
    <div>
      <h1 style={{ color: "#f1f5f9", fontSize: 20, fontWeight: 700, marginBottom: 16 }}>
        ML Model Training
      </h1>

      {/* KPIs */}
      <div style={{ display: "grid", gridTemplateColumns: "repeat(4, 1fr)", gap: 12, marginBottom: 20 }}>
        <KPICard label="Models Trained" value={`${models.filter((m) => m.trained).length} / ${models.length}`}
          accent="#38bdf8" />
        <KPICard label="Avg Accuracy" value={anyTrained ? fmtPct(avgAccuracy) : "—"} accent="#10b981" />
        <KPICard label="Asset Classes" value="4" sub="CEQ, LnD, MTN, OTC" />
        <KPICard label="Algorithm" value="Random Forest" sub="100 trees, balanced" />
      </div>

      {/* Model cards */}
      <div style={{ display: "grid", gridTemplateColumns: "repeat(4, 1fr)", gap: 16, marginBottom: 20 }}>
        {loading ? [1, 2, 3, 4].map((i) => (
          <div key={i} style={{ ...s.card(""), opacity: 0.4, height: 120 }} />
        )) : models.map((m) => <ModelCard key={m.asset_class} m={m} />)}
      </div>

      {/* Actions */}
      <div style={{ display: "flex", gap: 12, marginBottom: 24 }}>
        <button style={s.trainBtn(training)} disabled={training} onClick={train}>
          <Brain size={16} />
          {training ? "Training models…" : "Train All Models"}
        </button>
        {anyTrained && (
          <button style={{ ...s.trainBtn(false), background: "#334155" }} onClick={score}>
            <RefreshCw size={16} />Re-score All Breaks
          </button>
        )}
      </div>

      {/* Training results */}
      {results && (
        <Section title="Training Results" style={{ marginBottom: 20 }}>
          <table style={s.table}>
            <thead>
              <tr>
                {["Asset Class", "Status", "Training Samples", "Test Samples", "Accuracy", "Precision", "Recall"].map((h) => (
                  <th key={h} style={s.th}>{h}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {results.map((r) => (
                <tr key={r.asset_class}
                  onMouseEnter={(e) => e.currentTarget.style.background = "#243044"}
                  onMouseLeave={(e) => e.currentTarget.style.background = ""}>
                  <td style={{ ...s.td }}>
                    <span style={s.acBadge(r.asset_class)}>{r.asset_class}</span>
                  </td>
                  <td style={{ ...s.td }}>
                    <span style={{
                      color: r.status === "OK" ? "#10b981"
                           : r.status === "SKIPPED" ? "#f59e0b" : "#f43f5e",
                      fontWeight: 600,
                    }}>
                      {r.status}
                    </span>
                    {r.message && <span style={{ color: "#64748b", marginLeft: 6 }}>{r.message}</span>}
                  </td>
                  <td style={{ ...s.td, textAlign: "right" }}>{fmt(r.n_train)}</td>
                  <td style={{ ...s.td, textAlign: "right" }}>{fmt(r.n_test)}</td>
                  <td style={{ ...s.td, textAlign: "right", color: "#10b981" }}>{fmtPct(r.accuracy)}</td>
                  <td style={{ ...s.td, textAlign: "right" }}>{fmtPct(r.precision)}</td>
                  <td style={{ ...s.td, textAlign: "right" }}>{fmtPct(r.recall)}</td>
                </tr>
              ))}
            </tbody>
          </table>

          {/* Feature importances per model */}
          {results.filter((r) => r.top_features).map((r) => (
            <div key={r.asset_class} style={{ marginTop: 16 }}>
              <div style={{ color: "#94a3b8", fontSize: 12, fontWeight: 600, marginBottom: 8 }}>
                {r.asset_class} – Top Feature Importances
              </div>
              <div style={{ display: "flex", flexWrap: "wrap", gap: 6 }}>
                {r.top_features.slice(0, 8).map((f) => (
                  <div key={f.feature} style={{
                    background: "#0f172a", border: "1px solid #334155",
                    borderRadius: 5, padding: "4px 10px", fontSize: 11,
                    display: "flex", gap: 8, alignItems: "center",
                  }}>
                    <span style={{ color: "#94a3b8" }}>{f.feature}</span>
                    <span style={{ color: "#38bdf8", fontWeight: 600 }}>{(f.importance * 100).toFixed(1)}%</span>
                  </div>
                ))}
              </div>
            </div>
          ))}
        </Section>
      )}

      {/* Model details table */}
      <Section title="Model Registry">
        <div style={{ overflowX: "auto" }}>
          <table style={s.table}>
            <thead>
              <tr>
                {["Asset Class", "Status", "Last Trained", "Accuracy", "Samples", "Model File"].map((h) => (
                  <th key={h} style={s.th}>{h}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {models.map((m) => (
                <tr key={m.asset_class}
                  onMouseEnter={(e) => e.currentTarget.style.background = "#243044"}
                  onMouseLeave={(e) => e.currentTarget.style.background = ""}>
                  <td style={s.td}><span style={s.acBadge(m.asset_class)}>{m.asset_class}</span></td>
                  <td style={s.td}>
                    <span style={s.trainedBadge(m.trained)}>
                      {m.trained ? <CheckCircle size={11} /> : <XCircle size={11} />}
                      {m.trained ? "Trained" : "Not trained"}
                    </span>
                  </td>
                  <td style={{ ...s.td, color: "#94a3b8" }}>{fmtDate(m.last_trained)}</td>
                  <td style={{ ...s.td, textAlign: "right", color: "#10b981" }}>
                    {fmtPct(m.last_accuracy)}
                  </td>
                  <td style={{ ...s.td, textAlign: "right" }}>{fmt(m.last_n_train)}</td>
                  <td style={{ ...s.td, fontFamily: "monospace", fontSize: 10, color: "#475569" }}>
                    {m.trained ? m.model_path?.split("/").slice(-1)[0] : "—"}
                    {m.model_size_kb && <span style={{ color: "#334155" }}> ({m.model_size_kb} KB)</span>}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </Section>
    </div>
  );
}
