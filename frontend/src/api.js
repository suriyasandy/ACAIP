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

async function post(path) {
  const res = await fetch(`${BASE}${path}`, { method: "POST" });
  if (!res.ok) throw new Error(`API error ${res.status}`);
  return res.json();
}

// Dashboard
export const getSummary       = (f) => get("/dashboard/summary", f);
export const getPlatformBreakdown = (f) => get("/dashboard/platform-breakdown", f);
export const getAgeProfile    = (f) => get("/dashboard/age-profile", f);
export const getResolutionTrend = (days) => get("/dashboard/resolution-trend", { days });
export const getAssetClassBreakdown = () => get("/dashboard/asset-class-breakdown");

// Breaks
export const getBreaks        = (params) => get("/breaks", params);
export const getMaterialBreaks = () => get("/breaks/material");
export const getEmirBreaks    = () => get("/breaks/emir");

// Recs
export const getRecs          = () => get("/recs");
export const getRecDetail     = (id) => get(`/recs/${id}`);

// Jira
export const getJiraCoverage  = () => get("/jira/coverage");
export const getJiraDrafts    = () => get("/jira/drafts");
export const getJiraEpics     = () => get("/jira/epics");
export const getJiraCoverageGap = () => get("/jira/coverage-gap");
export const approveDraft     = (ref) => post(`/jira/approve/${ref}`);

// Themes
export const getThemeSummary  = () => get("/themes/summary");
export const getThemeTrend    = (days) => get("/themes/trend", { days });
export const getThemeCrosswalk = () => get("/themes/crosswalk");

// Health
export const getHealth        = () => get("/health");
