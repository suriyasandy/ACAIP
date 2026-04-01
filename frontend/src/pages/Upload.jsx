import React, { useState, useRef, useEffect } from "react";
import { UploadCloud, RefreshCw, CheckCircle, AlertCircle } from "lucide-react";
import Section from "../components/Section.jsx";

const PLATFORMS = ["TLM", "DUCO", "D3S"];
const ASSETS = ["CEQ", "LnD", "MTN", "OTC"];

const s = {
  grid: { display: "grid", gridTemplateColumns: "1fr 1fr", gap: 20, marginBottom: 24 },
  label: { color: "#94a3b8", fontSize: 12, display: "block", marginBottom: 6, fontWeight: 500 },
  input: {
    width: "100%", background: "#0f172a", border: "1px solid #334155",
    borderRadius: 6, color: "#e2e8f0", padding: "7px 12px", fontSize: 13,
  },
  select: {
    width: "100%", background: "#0f172a", border: "1px solid #334155",
    borderRadius: 6, color: "#e2e8f0", padding: "7px 10px", fontSize: 13, cursor: "pointer",
  },
  dropZone: (drag) => ({
    border: `2px dashed ${drag ? "#38bdf8" : "#334155"}`,
    borderRadius: 10, padding: "28px 20px", textAlign: "center",
    cursor: "pointer", marginBottom: 16, transition: "border-color 0.15s",
    background: drag ? "rgba(56,189,248,0.05)" : "transparent",
  }),
  btn: (disabled) => ({
    width: "100%", padding: "10px 0", borderRadius: 7, fontSize: 13, fontWeight: 600,
    cursor: disabled ? "not-allowed" : "pointer",
    background: disabled ? "#1e3a5f" : "#0ea5e9",
    color: disabled ? "#64748b" : "#fff", border: "none", marginTop: 12,
  }),
  fieldRow: { marginBottom: 12 },
  resultBadge: (ok) => ({
    display: "inline-flex", alignItems: "center", gap: 6,
    background: ok ? "rgba(16,185,129,0.15)" : "rgba(244,63,94,0.15)",
    color: ok ? "#6ee7b7" : "#fca5a5",
    borderRadius: 6, padding: "6px 12px", fontSize: 12, fontWeight: 600, marginTop: 10,
  }),
  table: { width: "100%", borderCollapse: "collapse", fontSize: 12 },
  th: { textAlign: "left", padding: "8px 10px", color: "#64748b", fontWeight: 600, borderBottom: "1px solid #334155" },
  td: { padding: "7px 10px", borderBottom: "1px solid #1e293b", color: "#e2e8f0" },
};

function DropZone({ onFile, file }) {
  const [drag, setDrag] = useState(false);
  const inputRef = useRef();

  const handleDrop = (e) => {
    e.preventDefault();
    setDrag(false);
    const f = e.dataTransfer.files[0];
    if (f) onFile(f);
  };

  return (
    <div
      style={s.dropZone(drag)}
      onClick={() => inputRef.current.click()}
      onDragOver={(e) => { e.preventDefault(); setDrag(true); }}
      onDragLeave={() => setDrag(false)}
      onDrop={handleDrop}
    >
      <UploadCloud size={28} color={drag ? "#38bdf8" : "#475569"} style={{ margin: "0 auto 8px" }} />
      {file ? (
        <span style={{ color: "#38bdf8", fontSize: 13 }}>{file.name}</span>
      ) : (
        <>
          <div style={{ color: "#94a3b8", fontSize: 13 }}>Drop file here or click to browse</div>
          <div style={{ color: "#475569", fontSize: 11, marginTop: 4 }}>CSV, XLSX, XLS</div>
        </>
      )}
      <input ref={inputRef} type="file" accept=".csv,.xlsx,.xls" style={{ display: "none" }}
        onChange={(e) => onFile(e.target.files[0])} />
    </div>
  );
}

function DailyBreakUpload({ onUploaded }) {
  const [file, setFile] = useState(null);
  const [form, setForm] = useState({ rec_id: "", rec_system: "TLM", rec_name: "", asset_class: "CEQ" });
  const [loading, setLoading] = useState(false);
  const [result, setResult] = useState(null);

  const set = (k, v) => setForm((f) => ({ ...f, [k]: v }));
  const canSubmit = file && form.rec_id.trim();

  const submit = async () => {
    if (!canSubmit) return;
    setLoading(true);
    setResult(null);
    const fd = new FormData();
    fd.append("file", file);
    Object.entries(form).forEach(([k, v]) => fd.append(k, v));
    try {
      const res = await fetch("/api/upload/daily_break", { method: "POST", body: fd });
      const data = await res.json();
      setResult(data);
      if (data.rows_loaded > 0) onUploaded();
    } catch (e) {
      setResult({ status: `Error: ${e.message}`, rows_loaded: 0 });
    } finally {
      setLoading(false);
    }
  };

  const ok = result && !String(result.status).startsWith("ERROR") && result.rows_loaded > 0;

  return (
    <Section title="Daily Break File">
      <DropZone file={file} onFile={setFile} />

      <div style={s.fieldRow}>
        <label style={s.label}>Rec ID *</label>
        <input style={s.input} placeholder="e.g. TLM-001" value={form.rec_id}
          onChange={(e) => set("rec_id", e.target.value)} />
      </div>
      <div style={s.fieldRow}>
        <label style={s.label}>Rec System</label>
        <select style={s.select} value={form.rec_system} onChange={(e) => set("rec_system", e.target.value)}>
          {PLATFORMS.map((p) => <option key={p}>{p}</option>)}
        </select>
      </div>
      <div style={s.fieldRow}>
        <label style={s.label}>Rec Name</label>
        <input style={s.input} placeholder="e.g. TLM Equity Settlement" value={form.rec_name}
          onChange={(e) => set("rec_name", e.target.value)} />
      </div>
      <div style={s.fieldRow}>
        <label style={s.label}>Asset Class</label>
        <select style={s.select} value={form.asset_class} onChange={(e) => set("asset_class", e.target.value)}>
          {ASSETS.map((a) => <option key={a}>{a}</option>)}
        </select>
      </div>

      <button style={s.btn(!canSubmit || loading)} disabled={!canSubmit || loading} onClick={submit}>
        {loading ? "Uploading…" : "Upload Break File"}
      </button>

      {result && (
        <div style={s.resultBadge(ok)}>
          {ok ? <CheckCircle size={14} /> : <AlertCircle size={14} />}
          {ok
            ? `${result.rows_loaded} rows loaded successfully`
            : result.status || "Upload failed"}
          {result.errors > 0 && ` · ${result.errors} error(s)`}
        </div>
      )}

      <div style={{ color: "#475569", fontSize: 11, marginTop: 10 }}>
        Required columns: <code style={{ color: "#94a3b8" }}>trade_ref, break_value, break_ccy</code>
        &nbsp;(report_date, break_type optional)
      </div>
    </Section>
  );
}

function MonthlyMIUpload({ onUploaded }) {
  const [file, setFile] = useState(null);
  const [loading, setLoading] = useState(false);
  const [result, setResult] = useState(null);

  const submit = async () => {
    if (!file) return;
    setLoading(true);
    setResult(null);
    const fd = new FormData();
    fd.append("file", file);
    try {
      const res = await fetch("/api/upload/monthly_mi", { method: "POST", body: fd });
      const data = await res.json();
      setResult(data);
      if (data.rows_loaded > 0) onUploaded();
    } catch (e) {
      setResult({ status: `Error: ${e.message}`, rows_loaded: 0 });
    } finally {
      setLoading(false);
    }
  };

  const ok = result && !String(result.status).startsWith("ERROR") && result.rows_loaded > 0;

  return (
    <Section title="Monthly MI File">
      <DropZone file={file} onFile={setFile} />

      <div style={{ color: "#64748b", fontSize: 12, marginBottom: 12, lineHeight: 1.6 }}>
        Upload a monthly management information export. Column names should match the
        breaks schema (e.g. <code style={{ color: "#94a3b8" }}>rec_id, trade_ref, break_value,
        break_ccy, report_date</code>). Extra columns are ignored; missing ones default to null.
      </div>

      <button style={s.btn(!file || loading)} disabled={!file || loading} onClick={submit}>
        {loading ? "Uploading…" : "Upload Monthly MI"}
      </button>

      {result && (
        <div style={s.resultBadge(ok)}>
          {ok ? <CheckCircle size={14} /> : <AlertCircle size={14} />}
          {ok
            ? `${result.rows_loaded} rows loaded (${result.rows_received} received)`
            : result.status || "Upload failed"}
        </div>
      )}
    </Section>
  );
}

function UploadLog({ refresh }) {
  const [rows, setRows] = useState([]);
  const [loading, setLoading] = useState(false);

  const load = async () => {
    setLoading(true);
    try {
      const data = await fetch("/api/upload/log").then((r) => r.json());
      setRows(Array.isArray(data) ? data : []);
    } finally {
      setLoading(false);
    }
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
          <RefreshCw size={13} />Refresh
        </button>
      }>
      {loading ? (
        <div style={{ color: "#64748b", textAlign: "center", padding: 24 }}>Loading…</div>
      ) : (
        <div style={{ overflowX: "auto" }}>
          <table style={s.table}>
            <thead>
              <tr>
                {["Timestamp", "Filename", "Source", "Received", "Loaded", "Errors", "Status"].map((h) => (
                  <th key={h} style={s.th}>{h}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {rows.map((r) => (
                <tr key={r.upload_id}
                  onMouseEnter={(e) => e.currentTarget.style.background = "#243044"}
                  onMouseLeave={(e) => e.currentTarget.style.background = ""}>
                  <td style={{ ...s.td, color: "#94a3b8", fontSize: 11 }}>{fmt(r.upload_ts)}</td>
                  <td style={{ ...s.td, fontFamily: "monospace", fontSize: 11 }}>{r.filename}</td>
                  <td style={s.td}>{r.source_detected}</td>
                  <td style={{ ...s.td, textAlign: "right" }}>{r.rows_received ?? "—"}</td>
                  <td style={{ ...s.td, textAlign: "right", color: "#10b981" }}>{r.rows_loaded ?? "—"}</td>
                  <td style={{ ...s.td, textAlign: "right", color: r.errors > 0 ? "#f43f5e" : "#64748b" }}>{r.errors ?? 0}</td>
                  <td style={{ ...s.td, fontSize: 11 }}>
                    <span style={{
                      color: String(r.status).startsWith("ERROR") ? "#f43f5e"
                           : r.status === "OK" ? "#10b981" : "#94a3b8",
                    }}>
                      {r.status}
                    </span>
                  </td>
                </tr>
              ))}
              {rows.length === 0 && (
                <tr><td colSpan={7} style={{ ...s.td, textAlign: "center", color: "#475569" }}>No uploads yet</td></tr>
              )}
            </tbody>
          </table>
        </div>
      )}
    </Section>
  );
}

export default function Upload() {
  const [logRefresh, setLogRefresh] = useState(0);
  const bump = () => setLogRefresh((n) => n + 1);

  return (
    <div>
      <h1 style={{ color: "#f1f5f9", fontSize: 20, fontWeight: 700, marginBottom: 16 }}>Upload Data</h1>

      <div style={s.grid}>
        <DailyBreakUpload onUploaded={bump} />
        <MonthlyMIUpload onUploaded={bump} />
      </div>

      <UploadLog refresh={logRefresh} />
    </div>
  );
}
