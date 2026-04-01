import pandas as pd

def flag(df):
    df = df.copy()
    def _level(row):
        if row.get("emir_flag") and (int(row.get("age_days") or 0) >= 1): return "EMIR-BREACH"
        if row.get("sla_breach") and row.get("threshold_breach"): return "URGENT"
        if row.get("sla_breach") or (float(row.get("ml_risk_score") or 0) >= 75): return "URGENT"
        if row.get("threshold_breach") or (int(row.get("age_days") or 0) >= 5): return "WARNING"
        return "INFO"
    df["escalation_flag"] = df.apply(_level, axis=1)
    df["bs_cert_ready"] = df["material_flag"].astype(bool) & df["threshold_breach"].astype(bool)
    def _priority(score):
        s = float(score or 0)
        if s >= 80: return "CRITICAL"
        elif s >= 60: return "HIGH"
        elif s >= 40: return "MEDIUM"
        return "LOW"
    df["jira_priority"] = df["ml_risk_score"].apply(_priority)
    return df
