import React, { useEffect, useState, useCallback } from "react";
import { getRecs, getRecConfigs, saveRecConfig, deleteRecConfig } from "../api.js";
import Section from "../components/Section.jsx";

const fmt = (n) => n == null ? "—" : Number(n).toLocaleString("en-GB", { maximumFractionDigits: 0 });
const fmtGBP = (n) => n == null ? "—" : "£" + (Number(n) >= 1e6 ? (Number(n) / 1e6).toFixed(1) + "M" : (Number(n) / 1e3).toFixed(0) + "K");
const fmtPct = (n) => n == null ? "—" : Number(n).toFixed(1) + "%";

const s = {
  table: { width: "100%", borderCollapse: "collapse", fontSize: 12 },
  th: { textAlign: "left", padding: "8px 10px", color: "#64748b", fontWeight: 600, borderBottom: "1px solid #334155", whiteSpace: "nowrap" },
  td: { padding: "8px 10px", borderBottom: "1px solid #1e293b", color: "#e2e8f0" },
};

function PctBar({ v, max = 100, color = "#38bdf8" }) {
  const pct = Math.min((v || 0) / max * 100, 100);
  return (
    <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
      <div style={{ flex: 1, height: 6, background: "#0f172a", borderRadius: 3, overflow: "hidden" }}>
        <div style={{ width: pct + "%", height: "100%", background: color, borderRadius: 3 }} />
      </div>
      <span style={{ color: "#94a3b8", fontSize: 11, minWidth: 40 }}>{fmtPct(v)}</span>
    </div>
  );
}

const EMPTY_CONFIG = {
  rec_id: "", rec_name: "", source_platform: "", asset_class: "CEQ",
  escalation_sla_days: "", threshold_abs_gbp: "", emir_flag: false, active: true,
};

function RecConfigPanel() {
  const [configs, setConfigs] = useState([]);
  const [editing, setEditing] = useState(null); // null | config object
  const [saving, setSaving] = useState(false);
  const [msg, setMsg] = useState(null);

  const load = useCallback(() => {
    getRecConfigs().then(setConfigs).catch(console.error);
  }, []);

  useEffect(() => { load(); }, [load]);

  const handleEdit = (cfg) => setEditing({ ...cfg });
  const handleNew = () => setEditing({ ...EMPTY_CONFIG });
  const handleCancel = () => { setEditing(null); setMsg(null); };

  const handleSave = async () => {
    if (!editing.rec_id) return setMsg("Rec ID is required");
    setSaving(true);
    try {
      await saveRecConfig(editing);
      setMsg("Saved");
      setEditing(null);
      load();
    } catch (e) {
      setMsg("Error: " + e.message);
    } finally {
      setSaving(false);
    }
  };

  const handleDelete = async (rec_id) => {
    if (!confirm(`Delete config for ${rec_id}?`)) return;
    try {
      await deleteRecConfig(rec_id);
      load();
    } catch (e) {
      console.error(e);
    }
  };

  const inputStyle = { background: "#0f172a", color: "#cbd5e1", border: "1px solid #334155", borderRadius: 4, padding: "5px 8px", fontSize: 13, width: "100%" };

  return (
    <div>
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 12 }}>
        <span style={{ color: "#94a3b8", fontSize: 13 }}>Configure SLA days, GBP thresholds, and flags per reconciliation.</span>
        <button onClick={handleNew} style={{ background: "#0ea5e9", color: "#fff", border: "none", borderRadius: 6, padding: "6px 14px", cursor: "pointer", fontWeight: 600, fontSize: 13 }}>
          + New Config
        </button>
      </div>

      {editing && (
        <div style={{ background: "#0f172a", border: "1px solid #334155", borderRadius: 8, padding: 16, marginBottom: 16 }}>
          <div style={{ fontWeight: 600, color: "#f1f5f9", marginBottom: 12 }}>{editing.rec_id ? `Edit: ${editing.rec_id}` : "New Rec Config"}</div>
          <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fill, minmax(160px, 1fr))", gap: 10 }}>
            {[
              ["rec_id", "Rec ID *"],
              ["rec_name", "Rec Name"],
              ["source_platform", "Platform"],
              ["asset_class", "Asset Class"],
              ["escalation_sla_days", "SLA Days"],
              ["threshold_abs_gbp", "GBP Threshold"],
            ].map(([field, label]) => (
              <div key={field}>
                <label style={{ fontSize: 11, color: "#64748b", display: "block", marginBottom: 3 }}>{label}</label>
                <input value={editing[field] || ""} onChange={(e) => setEditing((prev) => ({ ...prev, [field]: e.target.value }))} style={inputStyle} />
              </div>
            ))}
            <div style={{ display: "flex", alignItems: "center", gap: 8, paddingTop: 18 }}>
              <input type="checkbox" id="emir_flag" checked={!!editing.emir_flag} onChange={(e) => setEditing((prev) => ({ ...prev, emir_flag: e.target.checked }))} />
              <label htmlFor="emir_flag" style={{ fontSize: 13, color: "#cbd5e1" }}>EMIR</label>
            </div>
            <div style={{ display: "flex", alignItems: "center", gap: 8, paddingTop: 18 }}>
              <input type="checkbox" id="cfg_active" checked={!!editing.active} onChange={(e) => setEditing((prev) => ({ ...prev, active: e.target.checked }))} />
              <label htmlFor="cfg_active" style={{ fontSize: 13, color: "#cbd5e1" }}>Active</label>
            </div>
          </div>
          {msg && <div style={{ marginTop: 10, fontSize: 13, color: msg.startsWith("Error") ? "#f87171" : "#22c55e" }}>{msg}</div>}
          <div style={{ display: "flex", gap: 8, marginTop: 12 }}>
            <button onClick={handleSave} disabled={saving} style={{ background: "#0ea5e9", color: "#fff", border: "none", borderRadius: 6, padding: "6px 16px", cursor: "pointer", fontWeight: 600, fontSize: 13 }}>
              {saving ? "Saving…" : "Save"}
            </button>
            <button onClick={handleCancel} style={{ background: "#334155", color: "#cbd5e1", border: "none", borderRadius: 6, padding: "6px 14px", cursor: "pointer", fontSize: 13 }}>Cancel</button>
          </div>
        </div>
      )}

      <div style={{ overflowX: "auto" }}>
        <table style={s.table}>
          <thead>
            <tr>
              {["Rec ID", "Name", "Platform", "Asset Class", "SLA Days", "GBP Threshold", "EMIR", "Active", "Actions"].map((h) => (
                <th key={h} style={s.th}>{h}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {configs.map((c) => (
              <tr key={c.rec_id}>
                <td style={{ ...s.td, fontFamily: "monospace", color: "#38bdf8" }}>{c.rec_id}</td>
                <td style={s.td}>{c.rec_name || "—"}</td>
                <td style={{ ...s.td, color: "#94a3b8" }}>{c.source_platform || "—"}</td>
                <td style={s.td}>{c.asset_class || "—"}</td>
                <td style={{ ...s.td, textAlign: "right" }}>{c.escalation_sla_days != null ? `${c.escalation_sla_days}d` : "—"}</td>
                <td style={{ ...s.td, textAlign: "right" }}>{c.threshold_abs_gbp != null ? `£${Number(c.threshold_abs_gbp).toLocaleString()}` : "—"}</td>
                <td style={{ ...s.td, textAlign: "center" }}>{c.emir_flag ? <span style={{ color: "#a78bfa" }}>Yes</span> : "—"}</td>
                <td style={{ ...s.td, textAlign: "center" }}>{c.active ? <span style={{ color: "#22c55e" }}>Yes</span> : <span style={{ color: "#ef4444" }}>No</span>}</td>
                <td style={s.td}>
                  <div style={{ display: "flex", gap: 6 }}>
                    <button onClick={() => handleEdit(c)} style={{ background: "#1e3a5f", border: "1px solid #0ea5e944", color: "#38bdf8", borderRadius: 4, padding: "3px 10px", cursor: "pointer", fontSize: 12 }}>Edit</button>
                    <button onClick={() => handleDelete(c.rec_id)} style={{ background: "#3f1515", border: "1px solid #ef444444", color: "#f87171", borderRadius: 4, padding: "3px 10px", cursor: "pointer", fontSize: 12 }}>Delete</button>
                  </div>
                </td>
              </tr>
            ))}
            {configs.length === 0 && (
              <tr><td colSpan={9} style={{ ...s.td, textAlign: "center", color: "#64748b", padding: 24 }}>No configurations. Click "+ New Config" to add one.</td></tr>
            )}
          </tbody>
        </table>
      </div>
    </div>
  );
}

export default function RecsPage() {
  const [recs, setRecs] = useState([]);
  const [loading, setLoading] = useState(true);
  const [tab, setTab] = useState("stats");

  useEffect(() => {
    getRecs().then(setRecs).catch(console.error).finally(() => setLoading(false));
  }, []);

  const tabStyle = (active) => ({
    padding: "8px 20px", cursor: "pointer", fontSize: 14, fontWeight: active ? 600 : 400,
    color: active ? "#38bdf8" : "#94a3b8",
    borderBottom: active ? "2px solid #38bdf8" : "2px solid transparent",
    background: "transparent", border: "none", outline: "none",
  });

  return (
    <div>
      <h1 style={{ color: "#f1f5f9", fontSize: 20, fontWeight: 700, marginBottom: 16 }}>Reconciliations</h1>

      <div style={{ display: "flex", gap: 0, borderBottom: "1px solid #334155", marginBottom: 20 }}>
        <button style={tabStyle(tab === "stats")} onClick={() => setTab("stats")}>Stats</button>
        <button style={tabStyle(tab === "config")} onClick={() => setTab("config")}>Rec Configuration</button>
      </div>

      {tab === "stats" && (
        <Section>
          {loading ? (
            <div style={{ color: "#64748b", textAlign: "center", padding: 40 }}>Loading…</div>
          ) : (
            <div style={{ overflowX: "auto" }}>
              <table style={s.table}>
                <thead>
                  <tr>
                    {["Rec ID", "Name", "Platform", "Asset Class", "Total", "Open",
                      "GBP Exposure", "Material", "Avg Age", "Jira Coverage", "SLA Days", "EMIR"].map((h) => (
                      <th key={h} style={s.th}>{h}</th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {recs.map((r) => (
                    <tr key={r.rec_id}
                      onMouseEnter={(e) => e.currentTarget.style.background = "#243044"}
                      onMouseLeave={(e) => e.currentTarget.style.background = ""}>
                      <td style={{ ...s.td, fontFamily: "monospace", color: "#38bdf8" }}>{r.rec_id}</td>
                      <td style={s.td}>{r.rec_name}</td>
                      <td style={{ ...s.td, color: "#94a3b8" }}>{r.source_system}</td>
                      <td style={s.td}>
                        <span style={{
                          background: acColor(r.asset_class),
                          color: "#fff", fontSize: 10, fontWeight: 700,
                          borderRadius: 3, padding: "2px 6px",
                        }}>{r.asset_class}</span>
                      </td>
                      <td style={{ ...s.td, textAlign: "right" }}>{fmt(r.total_breaks)}</td>
                      <td style={{ ...s.td, textAlign: "right", color: "#f43f5e" }}>{fmt(r.open_breaks)}</td>
                      <td style={{ ...s.td, textAlign: "right" }}>{fmtGBP(r.total_gbp)}</td>
                      <td style={{ ...s.td, textAlign: "right", color: "#f59e0b" }}>{fmt(r.material_count)}</td>
                      <td style={{ ...s.td, textAlign: "right" }}>{r.avg_age != null ? r.avg_age + "d" : "—"}</td>
                      <td style={{ ...s.td, minWidth: 140 }}>
                        <PctBar v={r.jira_pct} color={r.jira_pct >= 95 ? "#10b981" : r.jira_pct >= 70 ? "#f59e0b" : "#f43f5e"} />
                      </td>
                      <td style={{ ...s.td, textAlign: "right", color: "#94a3b8" }}>{r.escalation_sla_days ?? "—"}d</td>
                      <td style={{ ...s.td, textAlign: "center" }}>
                        {r.emir_flag ? <span style={{ color: "#a78bfa", fontWeight: 700 }}>EMIR</span> : <span style={{ color: "#334155" }}>—</span>}
                      </td>
                    </tr>
                  ))}
                  {recs.length === 0 && (
                    <tr><td colSpan={12} style={{ ...s.td, textAlign: "center", color: "#64748b" }}>No data</td></tr>
                  )}
                </tbody>
              </table>
            </div>
          )}
        </Section>
      )}

      {tab === "config" && (
        <Section>
          <RecConfigPanel />
        </Section>
      )}
    </div>
  );
}

function acColor(ac) {
  return { CEQ: "#0ea5e9", LnD: "#10b981", MTN: "#f59e0b", OTC: "#a78bfa" }[ac] || "#64748b";
}
