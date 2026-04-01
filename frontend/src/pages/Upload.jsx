import React, { useEffect, useRef, useState } from "react";
import { uploadFile, getUploadLog, getSources } from "../api.js";

const s = {
  page: { color: "#f1f5f9" },
  section: {
    background: "#1e293b",
    border: "1px solid #334155",
    borderRadius: 10,
    padding: 20,
    marginBottom: 20,
  },
  heading: { color: "#f1f5f9", fontWeight: 600, fontSize: 15, marginBottom: 14 },
  dropzone: (dragging) => ({
    border: `2px dashed ${dragging ? "#38bdf8" : "#334155"}`,
    borderRadius: 10,
    padding: "36px 20px",
    textAlign: "center",
    cursor: "pointer",
    background: dragging ? "rgba(56,189,248,0.06)" : "transparent",
    transition: "all 0.2s",
    marginBottom: 16,
  }),
  dropText: { color: "#94a3b8", fontSize: 14 },
  uploadBtn: {
    background: "#0ea5e9",
    color: "#fff",
    border: "none",
    borderRadius: 6,
    padding: "8px 20px",
    fontSize: 14,
    fontWeight: 600,
    cursor: "pointer",
  },
  resultBox: (ok) => ({
    background: ok ? "rgba(52,211,153,0.08)" : "rgba(248,113,113,0.08)",
    border: `1px solid ${ok ? "#34d399" : "#f87171"}`,
    borderRadius: 8,
    padding: 14,
    marginTop: 12,
    fontSize: 13,
  }),
  infoBox: {
    background: "rgba(56,189,248,0.08)",
    border: "1px solid #38bdf8",
    borderRadius: 8,
    padding: 14,
    marginTop: 12,
    fontSize: 13,
    color: "#38bdf8",
  },
  table: { width: "100%", borderCollapse: "collapse", fontSize: 13 },
  th: { color: "#64748b", textAlign: "left", padding: "8px 10px", borderBottom: "1px solid #334155" },
  td: { color: "#cbd5e1", padding: "8px 10px", borderBottom: "1px solid #1e293b" },
  badge: (status) => ({
    padding: "2px 8px",
    borderRadius: 4,
    fontSize: 11,
    fontWeight: 600,
    background: status === "OK" ? "rgba(52,211,153,0.2)" : "rgba(251,146,60,0.2)",
    color: status === "OK" ? "#34d399" : "#fb923c",
  }),
  patternList: { listStyle: "none", padding: 0, margin: 0 },
  patternItem: { padding: "4px 0", color: "#94a3b8", fontSize: 13 },
  code: {
    background: "#0f172a",
    borderRadius: 4,
    padding: "1px 6px",
    fontFamily: "monospace",
    color: "#38bdf8",
    fontSize: 12,
  },
};

export default function Upload() {
  const [dragging, setDragging] = useState(false);
  const [file, setFile] = useState(null);
  const [uploading, setUploading] = useState(false);
  const [result, setResult] = useState(null);
  const [error, setError] = useState(null);
  const [log, setLog] = useState([]);
  const [sources, setSources] = useState([]);
  const fileRef = useRef();

  useEffect(() => {
    getUploadLog().then(setLog).catch(() => {});
    getSources().then(setSources).catch(() => {});
  }, []);

  function handleDrop(e) {
    e.preventDefault();
    setDragging(false);
    const f = e.dataTransfer.files[0];
    if (f) setFile(f);
  }

  async function handleUpload() {
    if (!file) return;
    setUploading(true);
    setResult(null);
    setError(null);
    try {
      const r = await uploadFile(file);
      setResult(r);
      const updated = await getUploadLog();
      setLog(updated);
    } catch (e) {
      setError(e.message);
    } finally {
      setUploading(false);
    }
  }

  return (
    <div style={s.page}>
      {/* Expected file patterns hint */}
      {sources.length > 0 && (
        <div style={s.section}>
          <div style={s.heading}>Registered File Patterns</div>
          {sources.map((src) =>
            src.recs.map((rec) => (
              <div key={rec.rec_id} style={{ marginBottom: 10 }}>
                <span style={{ color: "#64748b", fontSize: 12 }}>
                  {src.source_system} / {rec.rec_id} — {rec.rec_name}
                </span>
                <ul style={s.patternList}>
                  {rec.products.map((p) => (
                    <li key={p.product} style={s.patternItem}>
                      <code style={s.code}>{p.file_pattern}</code>
                      &nbsp;({p.product})
                    </li>
                  ))}
                </ul>
              </div>
            ))
          )}
        </div>
      )}

      {/* Upload area */}
      <div style={s.section}>
        <div style={s.heading}>Upload Break File (.xlsx)</div>
        <div
          style={s.dropzone(dragging)}
          onClick={() => fileRef.current.click()}
          onDragOver={(e) => { e.preventDefault(); setDragging(true); }}
          onDragLeave={() => setDragging(false)}
          onDrop={handleDrop}
        >
          <p style={s.dropText}>
            {file ? file.name : "Drag & drop an .xlsx file here, or click to browse"}
          </p>
        </div>
        <input
          ref={fileRef}
          type="file"
          accept=".xlsx"
          style={{ display: "none" }}
          onChange={(e) => setFile(e.target.files[0])}
        />
        <button style={s.uploadBtn} onClick={handleUpload} disabled={!file || uploading}>
          {uploading ? "Uploading…" : "Upload"}
        </button>

        {result && result.already_loaded && (
          <div style={s.infoBox}>
            File already loaded — skipping duplicate. (upload_id: {result.upload_id})
          </div>
        )}
        {result && !result.already_loaded && (
          <div style={s.resultBox(result.error_count === 0)}>
            <strong>{result.source_system} / {result.rec_id} / {result.product}</strong> &nbsp;|&nbsp; {result.file_date}
            <br />
            Rows received: <strong>{result.rows_received}</strong> &nbsp;|&nbsp;
            Loaded: <strong>{result.rows_loaded}</strong> &nbsp;|&nbsp;
            Errors: <strong style={{ color: result.error_count > 0 ? "#f87171" : "#34d399" }}>{result.error_count}</strong> &nbsp;|&nbsp;
            Warnings: <strong style={{ color: result.warning_count > 0 ? "#fbbf24" : "#34d399" }}>{result.warning_count}</strong>
            &nbsp;|&nbsp; Status: <strong>{result.status}</strong>
          </div>
        )}
        {error && <div style={s.resultBox(false)}>{error}</div>}
      </div>

      {/* Upload history */}
      <div style={s.section}>
        <div style={s.heading}>Upload History (last 100)</div>
        <table style={s.table}>
          <thead>
            <tr>
              {["Filename", "System", "Rec ID", "Product", "File Date", "Received", "Loaded", "Errors", "Warnings", "Status", "Uploaded At"].map((h) => (
                <th key={h} style={s.th}>{h}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {log.map((r, i) => (
              <tr key={i}>
                <td style={s.td}>{r.filename}</td>
                <td style={s.td}>{r.source_system}</td>
                <td style={s.td}>{r.rec_id}</td>
                <td style={s.td}>{r.product}</td>
                <td style={s.td}>{r.file_date}</td>
                <td style={s.td}>{r.rows_received}</td>
                <td style={s.td}>{r.rows_loaded}</td>
                <td style={{ ...s.td, color: r.error_count > 0 ? "#f87171" : "#94a3b8" }}>{r.error_count}</td>
                <td style={{ ...s.td, color: r.warning_count > 0 ? "#fbbf24" : "#94a3b8" }}>{r.warning_count}</td>
                <td style={s.td}><span style={s.badge(r.status)}>{r.status}</span></td>
                <td style={s.td}>{r.upload_ts}</td>
              </tr>
            ))}
            {log.length === 0 && (
              <tr><td colSpan={11} style={{ ...s.td, textAlign: "center", color: "#475569" }}>No uploads yet</td></tr>
            )}
          </tbody>
        </table>
      </div>
    </div>
  );
}
