import React, { useState } from "react";
import { Outlet, NavLink } from "react-router-dom";
import {
  LayoutDashboard, ListFilter, FileText, Tag, BarChart2, Menu, X,
  UploadCloud, Brain,
} from "lucide-react";

const NAV = [
  { to: "/dashboard", icon: LayoutDashboard, label: "Dashboard" },
  { to: "/breaks",    icon: ListFilter,      label: "Breaks" },
  { to: "/recs",      icon: BarChart2,       label: "Reconciliations" },
  { to: "/jira",      icon: FileText,        label: "Jira" },
  { to: "/themes",    icon: Tag,             label: "Themes" },
  { to: "/upload",    icon: UploadCloud,     label: "Upload" },
  { to: "/training",  icon: Brain,           label: "ML Training" },
];

const s = {
  shell: { display: "flex", height: "100vh", background: "#0f172a", overflow: "hidden" },
  sidebar: (open) => ({
    width: open ? 220 : 64,
    background: "#1e293b",
    borderRight: "1px solid #334155",
    display: "flex",
    flexDirection: "column",
    transition: "width 0.2s",
    overflow: "hidden",
    flexShrink: 0,
    zIndex: 10,
  }),
  logo: {
    display: "flex",
    alignItems: "center",
    gap: 10,
    padding: "18px 14px 14px",
    borderBottom: "1px solid #334155",
    minHeight: 60,
  },
  logoText: {
    color: "#38bdf8",
    fontWeight: 700,
    fontSize: 15,
    whiteSpace: "nowrap",
    overflow: "hidden",
    letterSpacing: 0.3,
  },
  toggle: {
    background: "none",
    border: "none",
    cursor: "pointer",
    color: "#94a3b8",
    display: "flex",
    alignItems: "center",
    flexShrink: 0,
  },
  nav: { flex: 1, padding: "12px 0" },
  link: (active) => ({
    display: "flex",
    alignItems: "center",
    gap: 12,
    padding: "10px 14px",
    color: active ? "#38bdf8" : "#94a3b8",
    background: active ? "rgba(56,189,248,0.1)" : "transparent",
    borderLeft: active ? "3px solid #38bdf8" : "3px solid transparent",
    textDecoration: "none",
    fontSize: 14,
    fontWeight: active ? 600 : 400,
    whiteSpace: "nowrap",
    transition: "background 0.15s, color 0.15s",
  }),
  main: {
    flex: 1,
    display: "flex",
    flexDirection: "column",
    overflow: "hidden",
    minWidth: 0,
  },
  topbar: {
    padding: "14px 24px",
    borderBottom: "1px solid #334155",
    display: "flex",
    alignItems: "center",
    justifyContent: "space-between",
    background: "#1e293b",
    minHeight: 60,
  },
  title: { color: "#f1f5f9", fontWeight: 600, fontSize: 16 },
  badge: {
    background: "#0ea5e9",
    color: "#fff",
    borderRadius: 4,
    fontSize: 11,
    padding: "2px 7px",
    fontWeight: 600,
  },
  content: { flex: 1, overflow: "auto", padding: "24px" },
};

export default function Layout() {
  const [open, setOpen] = useState(true);

  return (
    <div style={s.shell}>
      <aside style={s.sidebar(open)}>
        <div style={s.logo}>
          <button style={s.toggle} onClick={() => setOpen((o) => !o)} title="Toggle sidebar">
            {open ? <X size={18} /> : <Menu size={18} />}
          </button>
          {open && <span style={s.logoText}>ACAIP</span>}
        </div>
        <nav style={s.nav}>
          {NAV.map(({ to, icon: Icon, label }) => (
            <NavLink key={to} to={to} style={({ isActive }) => s.link(isActive)}>
              <Icon size={17} style={{ flexShrink: 0 }} />
              {open && label}
            </NavLink>
          ))}
        </nav>
        {open && (
          <div style={{ padding: "12px 14px", color: "#475569", fontSize: 11 }}>
            AI Break Management Platform
          </div>
        )}
      </aside>

      <main style={s.main}>
        <div style={s.topbar}>
          <span style={s.title}>AI-Powered Break Management</span>
          <span style={s.badge}>v1.0</span>
        </div>
        <div style={s.content}>
          <Outlet />
        </div>
      </main>
    </div>
  );
}
