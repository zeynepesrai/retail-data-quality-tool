from __future__ import annotations
from typing import Optional
import pandas as pd

from .utils import make_record_key
from .mapping import EXTRA_COLS, normalize_online_retail
from . import rules
from .report import export_excel

def run_dq(
    df: pd.DataFrame,
    table_name: str = "online_retail",
    reference_date: Optional[pd.Timestamp] = None,
):
    df = normalize_online_retail(df)
    record_key = make_record_key(df, ("INVOICE_NO","CLIENT_ID","SKU_CODE","INVOICE_DATE"))

    # Row-level checks
    row_fns = [
        rules.dq_01_completeness,
        rules.dq_02_invoice_date_parse,
        lambda d,t,rk,ec: rules.dq_03_invoice_date_future(d,t,rk,ec, reference_date=reference_date),
        rules.dq_06_numeric_parse,
        rules.dq_04_negative_values,
        rules.dq_07_volume_sales_mismatch,
        rules.dq_05_duplicate_line,
        rules.dq_10_invoice_mapping_inconsistency,
    ]

    row_issues = []
    for fn in row_fns:
        out = fn(df, table_name, record_key, EXTRA_COLS)
        if out is not None and not out.empty:
            row_issues.append(out)
    dq_row_issues = pd.concat(row_issues, ignore_index=True) if row_issues else pd.DataFrame()

    # Grouped checks
    grp_fns = [
        rules.dq_12g_customer_multiple_countries,
        rules.dq_13g_sku_multiple_names,
    ]
    grp_issues = []
    for gfn in grp_fns:
        gout = gfn(df, table_name=table_name)
        if gout is not None and not gout.empty:
            grp_issues.append(gout)
    dq_group_issues = pd.concat(grp_issues, ignore_index=True) if grp_issues else pd.DataFrame()

    # Label rows
    df_labeled = df.copy()
    if not dq_row_issues.empty:
        grp = dq_row_issues.groupby("record_key")["dq_rule_id"].apply(lambda x: ",".join(sorted(set(x))))
        df_labeled["dq_flag"] = record_key.isin(grp.index)
        df_labeled["dq_rule_list"] = record_key.map(lambda k: grp.get(k, ""))
    else:
        df_labeled["dq_flag"] = False
        df_labeled["dq_rule_list"] = ""

    return df_labeled, dq_row_issues, dq_group_issues

def run_and_export(
    input_csv: str,
    outdir: str = "outputs",
    sample_rows: Optional[int] = None,
):
    df = pd.read_csv(input_csv, encoding_errors="ignore")
    if sample_rows:
        df = df.head(sample_rows)

    # keep reference_date deterministic: max(invoice_date) if parseable else today
    ref = pd.to_datetime(df.get("InvoiceDate", pd.Series([], dtype="object")), errors="coerce")
    reference_date = ref.max()
    if pd.isna(reference_date):
        reference_date = pd.Timestamp.today().normalize()

    df_labeled, dq_row, dq_group = run_dq(df, reference_date=reference_date)

    import os
    os.makedirs(outdir, exist_ok=True)
    df_labeled.to_csv(os.path.join(outdir, "labeled_rows.csv"), index=False)

    excel_path = os.path.join(outdir, "DQ_Report.xlsx")
    export_excel(dq_row, dq_group, excel_path)

    return excel_path
