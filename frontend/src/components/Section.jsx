import React from "react";

const s = {
  section: {
    background: "#1e293b",
    border: "1px solid #334155",
    borderRadius: 10,
    padding: "16px 20px",
  },
  header: {
    display: "flex",
    alignItems: "center",
    justifyContent: "space-between",
    marginBottom: 16,
  },
  title: { color: "#f1f5f9", fontWeight: 600, fontSize: 14 },
};

export default function Section({ title, right, children, style }) {
  return (
    <div style={{ ...s.section, ...style }}>
      {title && (
        <div style={s.header}>
          <span style={s.title}>{title}</span>
          {right}
        </div>
      )}
      {children}
    </div>
  );
}
