import React from "react";

const s = {
  card: (accent) => ({
    background: "#1e293b",
    border: `1px solid ${accent || "#334155"}`,
    borderRadius: 10,
    padding: "16px 20px",
    display: "flex",
    flexDirection: "column",
    gap: 6,
  }),
  label: { color: "#94a3b8", fontSize: 12, fontWeight: 500, textTransform: "uppercase", letterSpacing: 0.5 },
  value: (accent) => ({ color: accent || "#f1f5f9", fontSize: 26, fontWeight: 700, lineHeight: 1.1 }),
  sub:   { color: "#64748b", fontSize: 12 },
};

export default function KPICard({ label, value, sub, accent }) {
  return (
    <div style={s.card(accent)}>
      <div style={s.label}>{label}</div>
      <div style={s.value(accent)}>{value ?? "—"}</div>
      {sub && <div style={s.sub}>{sub}</div>}
    </div>
  );
}
