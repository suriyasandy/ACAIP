import pandas as pd

def parse(filepath: str) -> pd.DataFrame:
    df = pd.read_csv(filepath, dtype=str, low_memory=False)
    df.columns = [c.strip() for c in df.columns]
    return df
