import React, { useState, useRef, useEffect } from "react";
import { useNavigate } from "react-router-dom";
import {
  UploadCloud, RefreshCw, CheckCircle, AlertCircle,
  Download, LayoutDashboard,
} from "lucide-react";
import Section from "../components/Section.jsx";
import ColumnMapper from "../components/ColumnMapper.jsx";
import {
  previewUpload, commitUpload, getUploadLog, getSchemaColumns,
} from "../api.js";

const st = {
  label:  { color: "#94a3b8", fontSize: 12, display: "block", marginBottom: 6, fontWeight: 500 },
  dropZone: (drag) => ({
    border: `2px dashed ${drag ? "#38bdf8" : "#334155"}`,
    borderRadius: 10, padding: "36px 20px", textAlign: "center",
    cursor: "pointer", marginBottom: 20, transition: "border-color 0.15s",
    background: drag ? "rgba(56,189,248,0.05)" : "transparent",
  }),
  radio: { display: "flex", gap: 24, marginBottom: 20 },
  radioItem: (active) => ({
    display: "flex", alignItems: "center", gap: 8, cursor: "pointer",
    color: active ? "#38bdf8" : "#94a3b8", fontSize: 13, fontWeight: active ? 600 : 400,
  }),
  btn: (primary, disabled) => ({
    padding: "10px 24px", borderRadius: 7, fontSize: 13, fontWeight: 600,
    cursor: disabled ? "not-allowed" : "pointer", border: "none",
    background: disabled ? "#1e3a5f" : primary ? "#0ea5e9" : "#1e293b",
    color:      disabled ? "#64748b"  : "#fff",
  }),
  stepBar: { display: "flex", gap: 0, marginBottom: 28 },
  step: (active, done) => ({
    flex: 1, padding: "8px 0", textAlign: "center", fontSize: 12, fontWeight: 600,
    background: done ? "#0369a1" : active ? "#0ea5e9" : "#1e293b",
    color:      done || active ? "#fff" : "#475569",
    borderRadius: 0,
  }),
  statBox: { background: "#1e293b", borderRadius: 8, padding: "14px 18px", marginBottom: 12 },
  statRow: { display: "flex", alignItems: "center", gap: 10, marginBottom: 6, fontSize: 13 },
  table: { width: "100%", borderCollapse: "collapse", fontSize: 11 },
  th: { textAlign: "left", padding: "7px 10px", color: "#64748b", fontWeight: 600,
        borderBottom: "1px solid #334155" },
  td: { padding: "6px 10px", borderBottom: "1px solid #1e293b", color: "#e2e8f0" },
};

// ── Drop zone ───────────────────────────────────────────────────────────────
function DropZone({ onFile, file }) {
  const [drag, setDrag] = useState(false);
  const ref = useRef();
  const handle = (e) => {
    e.preventDefault(); setDrag(false);
    const f = e.dataTransfer.files[0];
    if (f) onFile(f);
  };
  return (
    <div style={st.dropZone(drag)}
      onClick={() => ref.current.click()}
      onDragOver={(e) => { e.preventDefault(); setDrag(true); }}
      onDragLeave={() => setDrag(false)}
      onDrop={handle}>
      <UploadCloud size={32} color={drag ? "#38bdf8" : "#475569"} style={{ margin: "0 auto 10px" }} />
      {file
        ? <span style={{ color: "#38bdf8", fontSize: 14, fontWeight: 600 }}>{file.name}</span>
        : <>
            <div style={{ color: "#94a3b8", fontSize: 13 }}>Drop file here or click to browse</div>
            <div style={{ color: "#475569", fontSize: 11, marginTop: 4 }}>CSV, XLSX, XLS accepted</div>
          </>
      }
      <input ref={ref} type="file" accept=".csv,.xlsx,.xls" style={{ display: "none" }}
        onChange={(e) => onFile(e.target.files[0])} />
    </div>
  );
}

// ── Upload log (bottom panel) ────────────────────────────────────────────────
function UploadLog({ refresh }) {
  const [rows, setRows] = useState([]);
  const [loading, setLoading] = useState(false);

  const load = async () => {
    setLoading(true);
    try {
      const data = await getUploadLog();
      setRows(Array.isArray(data) ? data : []);
    } finally { setLoading(false); }
  };

  useEffect(() => { load(); }, [refresh]);

  const fmt = (ts) => ts ? new Date(ts).toLocaleString() : "—";

  return (
    <Section title="Upload History"
      right={
        <button onClick={load} style={{
          background: "none", border: "none", cursor: "pointer", color: "#64748b",
          display: "flex", alignItems: "center", gap: 4, fontSize: 12,
        }}>
          <RefreshCw size={13} /> Refresh
        </button>
      }>
      {loading
        ? <div style={{ color: "#64748b", textAlign: "center", padding: 24 }}>Loading…</div>
        : (
          <div style={{ overflowX: "auto" }}>
            <table style={st.table}>
              <thead>
                <tr>
                  {["Timestamp","Filename","Type","Received","Loaded","Errors","Status"].map((h) => (
                    <th key={h} style={st.th}>{h}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {rows.map((r) => (
                  <tr key={r.upload_id}
                    onMouseEnter={(e) => (e.currentTarget.style.background = "#243044")}
                    onMouseLeave={(e) => (e.currentTarget.style.background = "")}>
                    <td style={{ ...st.td, color: "#94a3b8" }}>{fmt(r.upload_ts)}</td>
                    <td style={{ ...st.td, fontFamily: "monospace" }}>{r.filename}</td>
                    <td style={st.td}>{r.source_detected}</td>
                    <td style={{ ...st.td, textAlign: "right" }}>{r.rows_received ?? "—"}</td>
                    <td style={{ ...st.td, textAlign: "right", color: "#10b981" }}>{r.rows_loaded ?? "—"}</td>
                    <td style={{ ...st.td, textAlign: "right", color: r.errors > 0 ? "#f43f5e" : "#64748b" }}>
                      {r.errors ?? 0}
                    </td>
                    <td style={{ ...st.td }}>
                      <span style={{ color: String(r.status).startsWith("ERROR") ? "#f43f5e" : r.status === "OK" ? "#10b981" : "#94a3b8" }}>
                        {r.status}
                      </span>
                    </td>
                  </tr>
                ))}
                {rows.length === 0 && (
                  <tr><td colSpan={7} style={{ ...st.td, textAlign: "center", color: "#475569" }}>No uploads yet</td></tr>
                )}
              </tbody>
            </table>
          </div>
        )}
    </Section>
  );
}

// ── Main wizard ───────────────────────────────────────────────────────────────
export default function Upload() {
  const navigate     = useNavigate();
  const [step, setStep]   = useState(1);           // 1 | 2 | 3
  const [file, setFile]   = useState(null);
  const [uploadType, setUploadType] = useState("monthly_mi");

  // Step 2 preview data
  const [preview,       setPreview]      = useState(null);  // {upload_id, headers, sample_rows, row_count, suggestions}
  const [schemaColumns, setSchemaColumns] = useState([]);
  const [previewLoading, setPreviewLoading] = useState(false);
  const [previewError,   setPreviewError]   = useState("");

  // Step 3 result
  const [result,         setResult]       = useState(null);
  const [commitLoading,  setCommitLoading] = useState(false);

  const [logRefresh, setLogRefresh] = useState(0);

  // Fetch schema columns once
  useEffect(() => {
    getSchemaColumns()
      .then((d) => setSchemaColumns(Array.isArray(d) ? d : []))
      .catch(() => {});
  }, []);

  // ── Step 1 → Step 2: preview upload ────────────────────────────────────────
  const handlePreview = async () => {
    if (!file) return;
    setPreviewLoading(true);
    setPreviewError("");
    try {
      const data = await previewUpload(file);
      if (data.error) { setPreviewError(data.error); return; }
      setPreview(data);
      setStep(2);
    } catch (e) {
      setPreviewError(e.message || "Preview failed");
    } finally {
      setPreviewLoading(false);
    }
  };

  // ── Step 2 → Step 3: commit ────────────────────────────────────────────────
  const handleCommit = async (mapping, recMeta, mappingName, saveYaml) => {
    if (!preview) return;
    setCommitLoading(true);
    try {
      const body = {
        upload_id:      preview.upload_id,
        upload_type:    uploadType,
        column_mapping: mapping,
        rec_meta:       uploadType === "daily_break" ? recMeta : {},
        save_yaml:      saveYaml,
        mapping_name:   mappingName,
      };
      const data = await commitUpload(body);
      setResult(data);
      setStep(3);
      setLogRefresh((n) => n + 1);
    } catch (e) {
      setResult({ status: `Error: ${e.message}`, rows_loaded: 0, errors: 1 });
      setStep(3);
    } finally {
      setCommitLoading(false);
    }
  };

  const reset = () => {
    setStep(1); setFile(null); setPreview(null);
    setResult(null); setPreviewError("");
  };

  const stepLabels = ["1. Select File", "2. Map Columns", "3. Result"];
  const ok = result && !String(result.status || "").startsWith("ERROR") && result.rows_loaded > 0;

  // ── YAML download helper ───────────────────────────────────────────────────
  const downloadYaml = () => {
    if (!result?.mapping_saved) return;
    fetch(`/api/schema/mapping/${result.mapping_saved.split("/").pop().replace(".yaml", "")}`)
      .then((r) => r.json())
      .then((data) => {
        const lines = [
          `source_name: "${data.source_name || ""}"`,
          `upload_type: ${data.upload_type || ""}`,
          `created: "${data.created || ""}"`,
          "column_mappings:",
          ...Object.entries(data.column_mappings || {}).map(([k, v]) => `  "${k}": ${v}`),
        ];
        if (data.rec_meta) {
          lines.push("rec_meta:");
          Object.entries(data.rec_meta).forEach(([k, v]) => lines.push(`  ${k}: "${v}"`));
        }
        const blob = new Blob([lines.join("\n")], { type: "text/yaml" });
        const a = document.createElement("a");
        a.href = URL.createObjectURL(blob);
        a.download = `${data.source_name || "mapping"}.yaml`;
        a.click();
      })
      .catch(() => {});
  };

  return (
    <div>
      <h1 style={{ color: "#f1f5f9", fontSize: 20, fontWeight: 700, marginBottom: 20 }}>Upload Data</h1>

      {/* Step indicator */}
      <div style={st.stepBar}>
        {stepLabels.map((label, i) => (
          <div key={i} style={{
            ...st.step(step === i + 1, step > i + 1),
            borderRadius: i === 0 ? "8px 0 0 8px" : i === stepLabels.length - 1 ? "0 8px 8px 0" : 0,
          }}>
            {label}
          </div>
        ))}
      </div>

      {/* ── STEP 1 ── */}
      {step === 1 && (
        <Section title="Upload File">
          <DropZone file={file} onFile={setFile} />

          <div style={st.radio}>
            {[
              { val: "monthly_mi",  label: "Monthly MI File" },
              { val: "daily_break", label: "Daily Break File" },
            ].map(({ val, label }) => (
              <label key={val} style={st.radioItem(uploadType === val)}>
                <input type="radio" value={val} checked={uploadType === val}
                  onChange={() => setUploadType(val)}
                  style={{ accentColor: "#0ea5e9" }} />
                {label}
              </label>
            ))}
          </div>

          {previewError && (
            <div style={{ color: "#f43f5e", fontSize: 12, marginBottom: 12, display: "flex", gap: 6, alignItems: "center" }}>
              <AlertCircle size={13} /> {previewError}
            </div>
          )}

          <button
            style={{ ...st.btn(true, !file || previewLoading), width: "100%" }}
            disabled={!file || previewLoading}
            onClick={handlePreview}>
            {previewLoading ? "Reading file…" : "Upload & Preview →"}
          </button>

          <div style={{ color: "#475569", fontSize: 11, marginTop: 12, lineHeight: 1.6 }}>
            <strong style={{ color: "#64748b" }}>Monthly MI</strong> — any column names; you will map them in the next step.
            Model training data source.<br />
            <strong style={{ color: "#64748b" }}>Daily Break</strong> — per-rec file; inference runs immediately if a trained model exists.
          </div>
        </Section>
      )}

      {/* ── STEP 2 ── */}
      {step === 2 && preview && (
        <Section title={`Map Columns — ${preview.filename} (${preview.row_count.toLocaleString()} rows)`}>
          <ColumnMapper
            headers={preview.headers}
            sampleRows={preview.sample_rows}
            uploadType={uploadType}
            schemaColumns={schemaColumns}
            suggestions={preview.suggestions || {}}
            onCommit={handleCommit}
            onBack={() => setStep(1)}
            loading={commitLoading}
          />
        </Section>
      )}

      {/* ── STEP 3 ── */}
      {step === 3 && result && (
        <Section title="Upload Complete">
          <div style={st.statBox}>
            <div style={{ display: "flex", alignItems: "center", gap: 10, marginBottom: 12 }}>
              {ok
                ? <CheckCircle size={20} color="#10b981" />
                : <AlertCircle size={20} color="#f43f5e" />}
              <span style={{ color: ok ? "#10b981" : "#f43f5e", fontWeight: 700, fontSize: 15 }}>
                {ok ? "Ingestion successful" : "Ingestion failed"}
              </span>
            </div>

            <div style={st.statRow}>
              <span style={{ color: "#64748b", minWidth: 140 }}>Rows received</span>
              <span style={{ color: "#e2e8f0" }}>{result.rows_received?.toLocaleString() ?? "—"}</span>
            </div>
            <div style={st.statRow}>
              <span style={{ color: "#64748b", minWidth: 140 }}>Rows loaded</span>
              <span style={{ color: "#10b981", fontWeight: 600 }}>{result.rows_loaded?.toLocaleString() ?? "—"}</span>
            </div>
            {result.errors > 0 && (
              <div style={st.statRow}>
                <span style={{ color: "#64748b", minWidth: 140 }}>Errors</span>
                <span style={{ color: "#f43f5e" }}>{result.errors}</span>
              </div>
            )}

            {result.inference_run && (
              <>
                <div style={{ borderTop: "1px solid #334155", margin: "10px 0" }} />
                <div style={st.statRow}>
                  <span style={{ color: "#64748b", minWidth: 140 }}>ML inference</span>
                  <span style={{ color: "#38bdf8" }}>
                    {result.rows_scored?.toLocaleString()} rows scored
                    {result.avg_risk_score != null && ` · avg risk ${result.avg_risk_score}`}
                  </span>
                </div>
              </>
            )}

            {result.mapping_saved && (
              <div style={st.statRow}>
                <span style={{ color: "#64748b", minWidth: 140 }}>Mapping saved</span>
                <span style={{ color: "#a78bfa", fontFamily: "monospace", fontSize: 11 }}>
                  {result.mapping_saved}
                </span>
              </div>
            )}

            {!ok && result.status && (
              <div style={{ color: "#f43f5e", fontSize: 12, marginTop: 8 }}>{result.status}</div>
            )}
          </div>

          {/* Actions */}
          <div style={{ display: "flex", gap: 12, flexWrap: "wrap" }}>
            <button style={st.btn(true, false)} onClick={() => navigate("/")}>
              <span style={{ display: "flex", alignItems: "center", gap: 6 }}>
                <LayoutDashboard size={14} /> View Dashboard
              </span>
            </button>
            <button style={st.btn(false, false)} onClick={reset}>
              Upload Another File
            </button>
            {result.mapping_saved && (
              <button style={st.btn(false, false)} onClick={downloadYaml}>
                <span style={{ display: "flex", alignItems: "center", gap: 6 }}>
                  <Download size={14} /> Download YAML
                </span>
              </button>
            )}
          </div>
        </Section>
      )}

      {/* Upload log always visible */}
      <div style={{ marginTop: 28 }}>
        <UploadLog refresh={logRefresh} />
      </div>
    </div>
  );
}
