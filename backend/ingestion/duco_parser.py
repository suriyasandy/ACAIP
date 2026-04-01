import json
import pandas as pd

def parse(filepath: str) -> pd.DataFrame:
    with open(filepath, "r") as f:
        data = json.load(f)
    if isinstance(data, list):
        rows = data
    elif isinstance(data, dict):
        rows = data.get("breaks", data.get("data", data.get("records", [])))
        if not isinstance(rows, list):
            rows = [data]
    else:
        rows = []
    df = pd.json_normalize(rows)
    df.columns = [c.strip() for c in df.columns]
    return df
