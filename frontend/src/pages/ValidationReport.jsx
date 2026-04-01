import React, { useEffect, useState, useCallback } from "react";
import { getValidationErrors } from "../api.js";

const SEVERITIES = ["", "ERROR", "WARNING"];
const ERROR_TYPES = [
  "", "MISSING_TRADE_ID", "MISSING_AS_AT_DATE", "MISSING_BREAK_DATE",
  "NEGATIVE_AGE", "CCY_MISMATCH", "MISSING_BREAK_AMOUNT", "UNKNOWN_PRODUCT", "MISSING_COLUMN",
];

const s = {
  page: { color: "#f1f5f9" },
  filterBar: {
    display: "flex",
    flexWrap: "wrap",
    gap: 10,
    marginBottom: 16,
    alignItems: "center",
  },
  select: {
    background: "#1e293b",
    border: "1px solid #334155",
    borderRadius: 6,
    color: "#f1f5f9",
    padding: "5px 10px",
    fontSize: 13,
    outline: "none",
  },
  input: {
    background: "#1e293b",
    border: "1px solid #334155",
    borderRadius: 6,
    color: "#f1f5f9",
    padding: "5px 10px",
    fontSize: 13,
    outline: "none",
  },
  btn: (primary) => ({
    background: primary ? "#0ea5e9" : "#1e293b",
    color: primary ? "#fff" : "#94a3b8",
    border: "1px solid",
    borderColor: primary ? "#0ea5e9" : "#334155",
    borderRadius: 6,
    padding: "5px 14px",
    fontSize: 13,
    cursor: "pointer",
    fontWeight: primary ? 600 : 400,
  }),
  summaryRow: { display: "flex", gap: 12, marginBottom: 16, flexWrap: "wrap" },
  chip: (color) => ({
    background: `${color}20`,
    border: `1px solid ${color}`,
    color,
    borderRadius: 6,
    padding: "4px 12px",
    fontSize: 12,
    fontWeight: 600,
  }),
  table: { width: "100%", borderCollapse: "collapse", fontSize: 12 },
  th: { color: "#64748b", textAlign: "left", padding: "8px 10px", borderBottom: "1px solid #334155", whiteSpace: "nowrap" },
  td: { color: "#cbd5e1", padding: "7px 10px", borderBottom: "1px solid #1e293b", verticalAlign: "top" },
  badge: (sev) => ({
    padding: "2px 7px",
    borderRadius: 4,
    fontSize: 11,
    fontWeight: 700,
    background: sev === "ERROR" ? "rgba(248,113,113,0.2)" : "rgba(251,191,36,0.2)",
    color: sev === "ERROR" ? "#f87171" : "#fbbf24",
    whiteSpace: "nowrap",
  }),
  pagination: { display: "flex", gap: 8, alignItems: "center", marginTop: 14, justifyContent: "center" },
  err: { color: "#f87171", fontSize: 13 },
  detail: { color: "#94a3b8", fontSize: 11, marginTop: 2 },
};

export default function ValidationReport() {
  const [severity, setSeverity] = useState("");
  const [errorType, setErrorType] = useState("");
  const [recId, setRecId] = useState("");
  const [sourceFile, setSourceFile] = useState("");
  const [page, setPage] = useState(1);
  const PAGE_SIZE = 50;

  const [rows, setRows] = useState([]);
  const [total, setTotal] = useState(0);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);

  const load = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const params = {
        page,
        page_size: PAGE_SIZE,
        ...(severity ? { severity } : {}),
        ...(errorType ? { error_type: errorType } : {}),
        ...(recId ? { rec_id: recId } : {}),
        ...(sourceFile ? { source_file: sourceFile } : {}),
      };
      const data = await getValidationErrors(params);
      setRows(data.rows || []);
      setTotal(data.total || 0);
    } catch (e) {
      setError(e.message);
    } finally {
      setLoading(false);
    }
  }, [severity, errorType, recId, sourceFile, page]);

  useEffect(() => { load(); }, [load]);

  function handleFilter() {
    setPage(1);
    load();
  }

  function exportCsv() {
    const headers = ["id", "source_file", "source_system", "rec_id", "file_date", "row_number", "trade_id", "error_type", "error_detail", "severity", "load_ts"];
    const lines = [headers.join(",")];
    for (const r of rows) {
      lines.push(headers.map((h) => JSON.stringify(r[h] ?? "")).join(","));
    }
    const blob = new Blob([lines.join("\n")], { type: "text/csv" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = "validation_errors.csv";
    a.click();
    URL.revokeObjectURL(url);
  }

  const errorCount   = rows.filter((r) => r.severity === "ERROR").length;
  const warningCount = rows.filter((r) => r.severity === "WARNING").length;
  const totalPages   = Math.max(1, Math.ceil(total / PAGE_SIZE));

  return (
    <div style={s.page}>
      {/* Filter bar */}
      <div style={s.filterBar}>
        <select style={s.select} value={severity} onChange={(e) => setSeverity(e.target.value)}>
          {SEVERITIES.map((v) => <option key={v} value={v}>{v || "All Severities"}</option>)}
        </select>
        <select style={s.select} value={errorType} onChange={(e) => setErrorType(e.target.value)}>
          {ERROR_TYPES.map((v) => <option key={v} value={v}>{v || "All Types"}</option>)}
        </select>
        <input
          style={s.input} placeholder="Rec ID" value={recId}
          onChange={(e) => setRecId(e.target.value)}
        />
        <input
          style={s.input} placeholder="Source file" value={sourceFile}
          onChange={(e) => setSourceFile(e.target.value)}
        />
        <button style={s.btn(true)} onClick={handleFilter}>Filter</button>
        <button style={s.btn(false)} onClick={exportCsv} disabled={rows.length === 0}>Export CSV</button>
      </div>

      {/* Summary chips */}
      <div style={s.summaryRow}>
        <span style={s.chip("#94a3b8")}>Total: {total}</span>
        <span style={s.chip("#f87171")}>Errors: {errorCount} (this page)</span>
        <span style={s.chip("#fbbf24")}>Warnings: {warningCount} (this page)</span>
      </div>

      {error && <p style={s.err}>{error}</p>}

      {/* Table */}
      <div style={{ overflowX: "auto" }}>
        <table style={s.table}>
          <thead>
            <tr>
              {["Sev", "Type", "Rec ID", "File", "File Date", "Row #", "Trade ID", "Detail", "Loaded At"].map((h) => (
                <th key={h} style={s.th}>{h}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {rows.map((r) => (
              <tr key={r.id}>
                <td style={s.td}><span style={s.badge(r.severity)}>{r.severity}</span></td>
                <td style={s.td}>{r.error_type}</td>
                <td style={s.td}>{r.rec_id}</td>
                <td style={s.td}>{r.source_file}</td>
                <td style={s.td}>{r.file_date}</td>
                <td style={s.td}>{r.row_number}</td>
                <td style={s.td}>{r.trade_id}</td>
                <td style={s.td}>{r.error_detail}</td>
                <td style={s.td}>{r.load_ts}</td>
              </tr>
            ))}
            {rows.length === 0 && !loading && (
              <tr>
                <td colSpan={9} style={{ ...s.td, textAlign: "center", color: "#475569" }}>
                  No validation issues found
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </div>

      {/* Pagination */}
      <div style={s.pagination}>
        <button style={s.btn(false)} onClick={() => setPage((p) => Math.max(1, p - 1))} disabled={page === 1}>
          ← Prev
        </button>
        <span style={{ color: "#94a3b8", fontSize: 13 }}>Page {page} / {totalPages}</span>
        <button style={s.btn(false)} onClick={() => setPage((p) => Math.min(totalPages, p + 1))} disabled={page === totalPages}>
          Next →
        </button>
      </div>
    </div>
  );
}
