from __future__ import annotations
from dataclasses import dataclass
from typing import Optional, List
import pandas as pd

from .utils import to_num, is_blank

@dataclass(frozen=True)
class Issue:
    dq_rule_id: str
    dq_dimension: str
    severity: str
    column_name: str
    fail_reason: str

def _append_issue_rows(
    df: pd.DataFrame,
    mask: pd.Series,
    issue: Issue,
    table_name: str,
    record_key: pd.Series,
    extra_cols: List[str],
) -> pd.DataFrame:
    if mask is None or mask.sum() == 0:
        return pd.DataFrame()

    cols = [c for c in extra_cols if c in df.columns]
    out = df.loc[mask, cols].copy()
    out["dq_rule_id"] = issue.dq_rule_id
    out["dq_dimension"] = issue.dq_dimension
    out["severity"] = issue.severity
    out["table_name"] = table_name
    out["column_name"] = issue.column_name
    out["fail_reason"] = issue.fail_reason
    out["record_key"] = record_key.loc[mask].values
    out["sample_value"] = df.loc[mask, issue.column_name].astype(str).values if issue.column_name in df.columns else None
    return out

# -----------------------
# Row-level DQ checks
# -----------------------
def dq_01_completeness(df, table_name, record_key, extra_cols):
    mandatory = [c for c in ["INVOICE_NO","INVOICE_DATE","SKU_CODE","VOLUME","UNIT_PRICE"] if c in df.columns]
    issues = []
    for col in mandatory:
        issues.append(_append_issue_rows(
            df,
            is_blank(df[col]),
            Issue("DQ_01","completeness","high",col,f"Mandatory field missing: {col}"),
            table_name, record_key, extra_cols
        ))
    non_empty = [x for x in issues if not x.empty]
    return pd.concat(non_empty, ignore_index=True) if non_empty else pd.DataFrame()

def dq_02_invoice_date_parse(df, table_name, record_key, extra_cols):
    if "INVOICE_DATE" not in df.columns:
        return pd.DataFrame()
    raw = df["INVOICE_DATE"]
    parsed = pd.to_datetime(raw, errors="coerce")
    mask = parsed.isna() & (~is_blank(raw))
    return _append_issue_rows(
        df, mask,
        Issue("DQ_02","validity","high","INVOICE_DATE","INVOICE_DATE cannot be parsed as date"),
        table_name, record_key, extra_cols
    )

def dq_03_invoice_date_future(df, table_name, record_key, extra_cols, reference_date: Optional[pd.Timestamp]=None):
    if "INVOICE_DATE" not in df.columns:
        return pd.DataFrame()
    if reference_date is None:
        reference_date = pd.Timestamp.today().normalize()
    parsed = pd.to_datetime(df["INVOICE_DATE"], errors="coerce")
    mask = parsed.notna() & (parsed > reference_date)
    return _append_issue_rows(
        df, mask,
        Issue("DQ_03","validity","medium","INVOICE_DATE",f"INVOICE_DATE is after reference date ({reference_date.date()})"),
        table_name, record_key, extra_cols
    )

def dq_06_numeric_parse(df, table_name, record_key, extra_cols):
    numeric_cols = ["VOLUME","UNIT_PRICE","GROSS_SALES","NET_SALES","TOTAL_INVOICE"]
    issues = []
    for col in numeric_cols:
        if col not in df.columns:
            continue
        raw = df[col]
        parsed = to_num(raw)
        mask = parsed.isna() & (~is_blank(raw))
        sev = "high" if col in ["VOLUME","UNIT_PRICE","NET_SALES"] else "medium"
        issues.append(_append_issue_rows(
            df, mask,
            Issue("DQ_06","validity",sev,col,f"{col} cannot be parsed as numeric"),
            table_name, record_key, extra_cols
        ))
    non_empty = [x for x in issues if not x.empty]
    return pd.concat(non_empty, ignore_index=True) if non_empty else pd.DataFrame()

def dq_04_negative_values(df, table_name, record_key, extra_cols):
    issues = []
    vol = to_num(df["VOLUME"]) if "VOLUME" in df.columns else None
    price = to_num(df["UNIT_PRICE"]) if "UNIT_PRICE" in df.columns else None
    gross = to_num(df["GROSS_SALES"]) if "GROSS_SALES" in df.columns else None
    if vol is not None:
        issues.append(_append_issue_rows(
            df, vol < 0,
            Issue("DQ_04","validity","high","VOLUME","VOLUME < 0 (returns/cancellations or sign error)"),
            table_name, record_key, extra_cols
        ))
    if price is not None:
        issues.append(_append_issue_rows(
            df, price < 0,
            Issue("DQ_04","validity","high","UNIT_PRICE","UNIT_PRICE < 0 (pricing/sign error)"),
            table_name, record_key, extra_cols
        ))
    if gross is not None and vol is not None and price is not None:
        mask = (gross < 0) & (vol > 0) & (price > 0)
        issues.append(_append_issue_rows(
            df, mask,
            Issue("DQ_04A","consistency","high","GROSS_SALES","GROSS_SALES < 0 while VOLUME and UNIT_PRICE > 0 (sign mismatch)"),
            table_name, record_key, extra_cols
        ))
    non_empty = [x for x in issues if not x.empty]
    return pd.concat(non_empty, ignore_index=True) if non_empty else pd.DataFrame()

def dq_07_volume_sales_mismatch(df, table_name, record_key, extra_cols):
    if not {"VOLUME","NET_SALES"}.issubset(df.columns):
        return pd.DataFrame()
    vol = to_num(df["VOLUME"]).fillna(0)
    net = to_num(df["NET_SALES"]).fillna(0)
    eps = 1e-2
    vol_min = 1e-3
    issues = []
    issues.append(_append_issue_rows(
        df, (vol.abs() > vol_min) & (net.abs() <= eps),
        Issue("DQ_07","consistency","medium","NET_SALES",f"Quantity present but NET_SALES is ~0 (<= {eps})"),
        table_name, record_key, extra_cols
    ))
    issues.append(_append_issue_rows(
        df, (net.abs() > eps) & (vol.abs() <= vol_min),
        Issue("DQ_07","consistency","medium","VOLUME",f"NET_SALES present but VOLUME is ~0 (<= {vol_min})"),
        table_name, record_key, extra_cols
    ))
    non_empty = [x for x in issues if not x.empty]
    return pd.concat(non_empty, ignore_index=True) if non_empty else pd.DataFrame()

def dq_05_duplicate_line(df, table_name, record_key, extra_cols):
    if "INVOICE_NO" not in df.columns or "SKU_CODE" not in df.columns:
        return pd.DataFrame()
    sig = [c for c in ["INVOICE_NO","SKU_CODE","CLIENT_ID","VOLUME","UNIT_PRICE"] if c in df.columns]
    mask = df.duplicated(subset=sig, keep=False)
    return _append_issue_rows(
        df, mask,
        Issue("DQ_05","uniqueness","medium","INVOICE_NO",f"Duplicate invoice line detected (signature={sig})"),
        table_name, record_key, extra_cols
    )

def dq_10_invoice_mapping_inconsistency(df, table_name, record_key, extra_cols):
    if "INVOICE_NO" not in df.columns:
        return pd.DataFrame()
    issues = []
    for col in ["CLIENT_ID","COUNTRY"]:
        if col not in df.columns:
            continue
        nunq = df.groupby("INVOICE_NO")[col].nunique(dropna=True)
        bad_inv = nunq[nunq > 1].index
        mask = df["INVOICE_NO"].isin(bad_inv)
        issues.append(_append_issue_rows(
            df, mask,
            Issue("DQ_10","consistency","high",col,f"INVOICE_NO maps to multiple values of {col}"),
            table_name, record_key, extra_cols
        ))
    non_empty = [x for x in issues if not x.empty]
    return pd.concat(non_empty, ignore_index=True) if non_empty else pd.DataFrame()

# -----------------------
# Grouped DQ checks
# -----------------------
def dq_12g_customer_multiple_countries(df, table_name="online_retail"):
    if not {"CLIENT_ID","COUNTRY"}.issubset(df.columns):
        return pd.DataFrame()
    nunq = df.groupby("CLIENT_ID")["COUNTRY"].nunique(dropna=True)
    bad_ids = nunq[nunq > 1].index
    if len(bad_ids) == 0:
        return pd.DataFrame()
    sub = df[df["CLIENT_ID"].isin(bad_ids)]
    out = (
        sub.groupby("CLIENT_ID")
        .agg(
            mapped_values=("COUNTRY", lambda x: " | ".join(sorted(set(x.dropna().astype(str))))),
            distinct_mapped_count=("COUNTRY", lambda x: x.dropna().nunique()),
            affected_rows=("CLIENT_ID","size"),
        )
        .reset_index()
        .rename(columns={"CLIENT_ID":"entity_key_value"})
    )
    out["dq_rule_id"] = "DQ_12G"
    out["dq_dimension"] = "consistency"
    out["severity"] = "high"
    out["table_name"] = table_name
    out["entity_key_type"] = "CLIENT_ID"
    out["mapped_column"] = "COUNTRY"
    out["fail_reason"] = "CLIENT_ID maps to multiple COUNTRY values"
    return out

def dq_13g_sku_multiple_names(df, table_name="online_retail"):
    if not {"SKU_CODE","SKU_NAME"}.issubset(df.columns):
        return pd.DataFrame()
    nunq = df.groupby("SKU_CODE")["SKU_NAME"].nunique(dropna=True)
    bad = nunq[nunq > 1].index
    if len(bad) == 0:
        return pd.DataFrame()
    sub = df[df["SKU_CODE"].isin(bad)]
    out = (
        sub.groupby("SKU_CODE")
        .agg(
            mapped_values=("SKU_NAME", lambda x: " | ".join(sorted(set(x.dropna().astype(str))))),
            distinct_mapped_count=("SKU_NAME", lambda x: x.dropna().nunique()),
            affected_rows=("SKU_CODE","size"),
        )
        .reset_index()
        .rename(columns={"SKU_CODE":"entity_key_value"})
    )
    out["dq_rule_id"] = "DQ_13G"
    out["dq_dimension"] = "consistency"
    out["severity"] = "high"
    out["table_name"] = table_name
    out["entity_key_type"] = "SKU_CODE"
    out["mapped_column"] = "SKU_NAME"
    out["fail_reason"] = "SKU_CODE maps to multiple SKU_NAME values"
    return out
