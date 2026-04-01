const BASE = "/api";

async function get(path, params = {}) {
  const qs = new URLSearchParams();
  for (const [k, v] of Object.entries(params)) {
    if (Array.isArray(v)) v.forEach((i) => qs.append(k, i));
    else if (v !== null && v !== undefined && v !== "") qs.append(k, v);
  }
  const url = `${BASE}${path}${qs.toString() ? "?" + qs.toString() : ""}`;
  const res = await fetch(url);
  if (!res.ok) throw new Error(`API error ${res.status} on ${url}`);
  return res.json();
}

async function post(path, body) {
  const res = await fetch(`${BASE}${path}`, {
    method: "POST",
    headers: body ? { "Content-Type": "application/json" } : {},
    body: body ? JSON.stringify(body) : undefined,
  });
  if (!res.ok) throw new Error(`API error ${res.status}`);
  return res.json();
}

async function del(path) {
  const res = await fetch(`${BASE}${path}`, { method: "DELETE" });
  if (!res.ok) throw new Error(`API error ${res.status}`);
  return res.json();
}

// ── Dashboard ────────────────────────────────────────────────────────────────
export const getSummary           = (f) => get("/dashboard/summary", f);
export const getPlatformBreakdown = (f) => get("/dashboard/platform-breakdown", f);
export const getAgeProfile        = (f) => get("/dashboard/age-profile", f);
export const getResolutionTrend   = (days) => get("/dashboard/resolution-trend", { days });
export const getAssetClassBreakdown = () => get("/dashboard/asset-class-breakdown");

// ── Breaks ───────────────────────────────────────────────────────────────────
export const getBreaks        = (params) => get("/breaks", params);
export const getMaterialBreaks = () => get("/breaks/material");
export const getEmirBreaks    = () => get("/breaks/emir");

// ── Recs ─────────────────────────────────────────────────────────────────────
export const getRecs      = () => get("/recs");
export const getRecDetail = (id) => get(`/recs/${id}`);

// ── Jira ─────────────────────────────────────────────────────────────────────
export const getJiraCoverage   = () => get("/jira/coverage");
export const getJiraDrafts     = () => get("/jira/drafts");
export const getJiraEpics      = () => get("/jira/epics");
export const getJiraCoverageGap = () => get("/jira/coverage-gap");
export const approveDraft      = (ref) => post(`/jira/approve/${ref}`);

// ── Themes ───────────────────────────────────────────────────────────────────
export const getThemeSummary   = () => get("/themes/summary");
export const getThemeTrend     = (days) => get("/themes/trend", { days });
export const getThemeCrosswalk = () => get("/themes/crosswalk");

// ── Upload – two-step ────────────────────────────────────────────────────────
export async function previewUpload(file) {
  const fd = new FormData();
  fd.append("file", file);
  const res = await fetch(`${BASE}/upload/preview`, { method: "POST", body: fd });
  if (!res.ok) throw new Error(`Preview failed: ${res.status}`);
  return res.json();
}

export const commitUpload = (body) => post("/upload/commit", body);
export const getUploadLog = () => get("/upload/log");

// ── Schema / Column mapping ───────────────────────────────────────────────────
export const getSchemaColumns = () => get("/schema/columns");
export const getMappings      = () => get("/schema/mappings");
export const getMapping       = (name) => get(`/schema/mapping/${name}`);
export const saveMapping      = (body) => post("/schema/mapping", body);
export const deleteMapping    = (name) => del(`/schema/mapping/${name}`);

// ── Pipeline / ML ─────────────────────────────────────────────────────────────
export const getHealth       = () => get("/health");
export const getPipelineStatus = () => get("/pipeline/status");
export const trainModels     = () => post("/pipeline/train");
export const getModelStatus  = () => get("/pipeline/models");
export const scoreBreaks     = () => post("/pipeline/score");
