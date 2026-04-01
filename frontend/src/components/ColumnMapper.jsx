import React, { useState, useEffect } from "react";
import { getMappings, getMapping, deleteMapping } from "../api.js";

const PLATFORMS = ["TLM", "DUCO", "D3S"];
const ASSETS    = ["CEQ", "LnD", "MTN", "OTC"];

const st = {
  label:  { color: "#94a3b8", fontSize: 11, display: "block", marginBottom: 5, fontWeight: 500 },
  input:  { background: "#0f172a", border: "1px solid #334155", borderRadius: 6,
            color: "#e2e8f0", padding: "6px 10px", fontSize: 12, width: "100%" },
  select: { background: "#0f172a", border: "1px solid #334155", borderRadius: 6,
            color: "#e2e8f0", padding: "6px 8px", fontSize: 12, cursor: "pointer", width: "100%" },
  th:     { textAlign: "left", padding: "8px 12px", color: "#64748b", fontSize: 11,
            fontWeight: 600, borderBottom: "1px solid #334155", whiteSpace: "nowrap" },
  td:     { padding: "7px 12px", borderBottom: "1px solid #1e293b", verticalAlign: "middle" },
  tag:    { display: "inline-block", background: "rgba(56,189,248,0.15)", color: "#38bdf8",
            borderRadius: 4, padding: "2px 6px", fontSize: 10, fontWeight: 600 },
  sample: { color: "#64748b", fontSize: 11, maxWidth: 160, overflow: "hidden",
            textOverflow: "ellipsis", whiteSpace: "nowrap" },
  btn:    (primary, disabled) => ({
    padding: "8px 20px", borderRadius: 7, fontSize: 13, fontWeight: 600,
    cursor: disabled ? "not-allowed" : "pointer", border: "none",
    background: disabled ? "#1e3a5f" : primary ? "#0ea5e9" : "#1e293b",
    color:      disabled ? "#64748b"  : "#fff",
  }),
  section: { background: "#1e293b", borderRadius: 8, padding: "14px 16px", marginBottom: 16 },
  row:    { display: "flex", alignItems: "center", gap: 12, marginBottom: 10 },
};

/** Render comma-separated sample values from up to 3 sample rows */
function sampleValues(header, sampleRows) {
  const vals = sampleRows
    .map((r) => String(r[header] ?? "").trim())
    .filter(Boolean)
    .slice(0, 3);
  return vals.length ? vals.join(", ") : "—";
}

export default function ColumnMapper({
  headers,       // string[]
  sampleRows,    // object[]
  uploadType,    // "monthly_mi" | "daily_break"
  schemaColumns, // [{name, label, required, category}]
  suggestions,   // {header: schemaColName|null}
  onCommit,      // (mapping, recMeta, mappingName, saveYaml) => void
  onBack,        // () => void
  loading,       // bool
}) {
  // Build initial mapping from suggestions
  const [mapping, setMapping] = useState(() => {
    const m = {};
    headers.forEach((h) => { m[h] = suggestions?.[h] || ""; });
    return m;
  });

  const [recMeta, setRecMeta] = useState({
    rec_id: "", rec_system: "TLM", rec_name: "", asset_class: "CEQ",
  });
  const [mappingName, setMappingName] = useState("");
  const [saveYaml, setSaveYaml]       = useState(false);

  // Saved mapping controls
  const [savedMappings, setSavedMappings] = useState([]);
  const [loadingMap,    setLoadingMap]    = useState(false);

  useEffect(() => {
    getMappings()
      .then((d) => setSavedMappings(Array.isArray(d) ? d : []))
      .catch(() => {});
  }, []);

  const loadSavedMapping = async (name) => {
    if (!name) return;
    setLoadingMap(true);
    try {
      const data = await getMapping(name);
      const colMap = data.column_mappings || {};
      setMapping((prev) => {
        const next = { ...prev };
        // Apply saved mapping for headers that exist in this file
        Object.entries(colMap).forEach(([fileCol, schemaCol]) => {
          if (fileCol in next) next[fileCol] = schemaCol;
        });
        return next;
      });
      if (data.rec_meta) setRecMeta((r) => ({ ...r, ...data.rec_meta }));
      if (data.source_name) setMappingName(data.source_name);
    } catch (_) {}
    setLoadingMap(false);
  };

  const setMap = (header, value) =>
    setMapping((m) => ({ ...m, [header]: value }));

  const setRec = (k, v) =>
    setRecMeta((r) => ({ ...r, [k]: v }));

  // Schema options: "" = ignore, then all schema columns
  const schemaOptions = [
    { name: "", label: "— Ignore —" },
    ...schemaColumns,
  ];

  const requiredMapped = schemaColumns
    .filter((c) => c.required)
    .every((c) => Object.values(mapping).includes(c.name));

  const canCommit = requiredMapped &&
    (uploadType !== "daily_break" || recMeta.rec_id.trim());

  const handleCommit = () => {
    // Only include non-empty mappings
    const clean = {};
    Object.entries(mapping).forEach(([k, v]) => { if (v) clean[k] = v; });
    onCommit(clean, recMeta, mappingName, saveYaml);
  };

  return (
    <div>
      {/* ── Top controls ── */}
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 16 }}>
        <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
          <select
            style={{ ...st.select, width: 220 }}
            defaultValue=""
            onChange={(e) => loadSavedMapping(e.target.value)}
            disabled={loadingMap}
          >
            <option value="">Load saved mapping…</option>
            {savedMappings.map((m) => (
              <option key={m.name} value={m.name}>{m.label}</option>
            ))}
          </select>
          {loadingMap && <span style={{ color: "#64748b", fontSize: 11 }}>Loading…</span>}
        </div>

        <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
          <label style={{ ...st.label, margin: 0, display: "flex", alignItems: "center", gap: 6 }}>
            <input
              type="checkbox"
              checked={saveYaml}
              onChange={(e) => setSaveYaml(e.target.checked)}
              style={{ accentColor: "#0ea5e9" }}
            />
            <span style={{ color: "#94a3b8", fontSize: 12 }}>Save mapping as:</span>
          </label>
          <input
            style={{ ...st.input, width: 180 }}
            placeholder="e.g. TLM Monthly MI v1"
            value={mappingName}
            onChange={(e) => setMappingName(e.target.value)}
            disabled={!saveYaml}
          />
        </div>
      </div>

      {/* ── Mapping table ── */}
      <div style={{ overflowX: "auto", marginBottom: 16 }}>
        <table style={{ width: "100%", borderCollapse: "collapse" }}>
          <thead>
            <tr>
              <th style={st.th}>File Column</th>
              <th style={st.th}>Sample Values</th>
              <th style={st.th}>Maps to Schema Column</th>
            </tr>
          </thead>
          <tbody>
            {headers.map((h) => {
              const isSuggested = suggestions?.[h] && suggestions[h] === mapping[h];
              return (
                <tr key={h}
                  onMouseEnter={(e) => (e.currentTarget.style.background = "#243044")}
                  onMouseLeave={(e) => (e.currentTarget.style.background = "")}
                >
                  <td style={st.td}>
                    <span style={{ color: "#e2e8f0", fontSize: 12 }}>{h}</span>
                  </td>
                  <td style={{ ...st.td, ...st.sample }}
                    title={sampleValues(h, sampleRows)}>
                    {sampleValues(h, sampleRows)}
                  </td>
                  <td style={st.td}>
                    <div style={{ display: "flex", alignItems: "center", gap: 6 }}>
                      <select
                        style={{ ...st.select, maxWidth: 200 }}
                        value={mapping[h] || ""}
                        onChange={(e) => setMap(h, e.target.value)}
                      >
                        {schemaOptions.map((o) => (
                          <option key={o.name} value={o.name}>
                            {o.label}{o.required ? " *" : ""}
                          </option>
                        ))}
                      </select>
                      {isSuggested && (
                        <span style={st.tag}>auto</span>
                      )}
                    </div>
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>

      {/* Required columns hint */}
      {!requiredMapped && (
        <div style={{ color: "#f59e0b", fontSize: 11, marginBottom: 12 }}>
          Required columns not yet mapped:{" "}
          {schemaColumns
            .filter((c) => c.required && !Object.values(mapping).includes(c.name))
            .map((c) => c.label)
            .join(", ")}
        </div>
      )}

      {/* ── Daily Break rec metadata ── */}
      {uploadType === "daily_break" && (
        <div style={st.section}>
          <div style={{ color: "#94a3b8", fontSize: 12, fontWeight: 600, marginBottom: 12 }}>
            Rec Metadata (applies to all rows in this file)
          </div>
          <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr 2fr 1fr", gap: 12 }}>
            <div>
              <label style={st.label}>Rec ID *</label>
              <input style={st.input} placeholder="e.g. TLM-001" value={recMeta.rec_id}
                onChange={(e) => setRec("rec_id", e.target.value)} />
            </div>
            <div>
              <label style={st.label}>Rec System</label>
              <select style={st.select} value={recMeta.rec_system}
                onChange={(e) => setRec("rec_system", e.target.value)}>
                {PLATFORMS.map((p) => <option key={p}>{p}</option>)}
              </select>
            </div>
            <div>
              <label style={st.label}>Rec Name</label>
              <input style={st.input} placeholder="e.g. TLM Equity Settlement" value={recMeta.rec_name}
                onChange={(e) => setRec("rec_name", e.target.value)} />
            </div>
            <div>
              <label style={st.label}>Asset Class</label>
              <select style={st.select} value={recMeta.asset_class}
                onChange={(e) => setRec("asset_class", e.target.value)}>
                {ASSETS.map((a) => <option key={a}>{a}</option>)}
              </select>
            </div>
          </div>
          {!recMeta.rec_id.trim() && (
            <div style={{ color: "#f59e0b", fontSize: 11, marginTop: 8 }}>
              Rec ID is required for daily break files.
            </div>
          )}
        </div>
      )}

      {/* ── Actions ── */}
      <div style={{ display: "flex", justifyContent: "space-between", marginTop: 8 }}>
        <button style={st.btn(false, false)} onClick={onBack}>← Back</button>
        <button style={st.btn(true, !canCommit || loading)} disabled={!canCommit || loading}
          onClick={handleCommit}>
          {loading ? "Ingesting…" : "Apply & Ingest →"}
        </button>
      </div>
    </div>
  );
}
