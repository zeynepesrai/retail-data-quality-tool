from __future__ import annotations
import pandas as pd

NULL_LIKE = {"", "NULL", "NONE", "N/A", "NA", "NAN", "-", "?"}

def to_num(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s, errors="coerce")

def is_blank(s: pd.Series) -> pd.Series:
    if s is None:
        return pd.Series(False)
    ss = s.astype(str).str.strip().str.upper()
    return s.isna() | ss.isin(NULL_LIKE)

def make_record_key(df: pd.DataFrame, key_cols: tuple[str, ...]) -> pd.Series:
    cols = [c for c in key_cols if c in df.columns]
    if not cols:
        return pd.Series(["ROW_" + str(i) for i in df.index], index=df.index)
    return df[cols].astype(str).agg("|".join, axis=1)
