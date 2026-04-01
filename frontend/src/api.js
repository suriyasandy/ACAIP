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

// ── Dashboard ────────────────────────────────────────────────────────────────
export const getKpis               = (f) => get("/dashboard/kpis", f || {});
export const getAgeProfile         = (f) => get("/dashboard/age-profile", f || {});
export const getProductBreakdown   = (f) => get("/dashboard/product-breakdown", f || {});
export const getTrend              = (f) => get("/dashboard/trend", f || {});
export const getValidationSummary  = ()  => get("/dashboard/validation-summary");

// ── Validation ────────────────────────────────────────────────────────────────
export const getValidationErrors = (params) => get("/validation/errors", params || {});

// ── Upload ────────────────────────────────────────────────────────────────────
export async function uploadFile(file) {
  const fd = new FormData();
  fd.append("file", file);
  const res = await fetch(`${BASE}/upload`, { method: "POST", body: fd });
  if (!res.ok) {
    const err = await res.json().catch(() => ({}));
    throw new Error(err.error || `Upload failed: ${res.status}`);
  }
  return res.json();
}

export const getUploadLog = () => get("/upload/log");

// ── Config ────────────────────────────────────────────────────────────────────
export const getSources = () => get("/config/sources");
