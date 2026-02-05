from __future__ import annotations
import pandas as pd

# Canonical schema used by rules
CANON = {
    "INVOICE_NO": "InvoiceNo",
    "INVOICE_DATE": "InvoiceDate",
    "SKU_CODE": "StockCode",
    "SKU_NAME": "Description",
    "VOLUME": "Quantity",
    "UNIT_PRICE": "UnitPrice",
    "CLIENT_ID": "CustomerID",
    "COUNTRY": "Country",
}

EXTRA_COLS = [
    "INVOICE_NO","INVOICE_DATE","SKU_CODE","SKU_NAME","VOLUME","UNIT_PRICE","CLIENT_ID","COUNTRY"
]

def normalize_online_retail(df: pd.DataFrame) -> pd.DataFrame:
    """Map common Online Retail column names to a canonical set.

    If your dataset already has canonical names, this is basically a no-op.
    """
    out = df.copy()
    # rename where present
    rename_map = {src: canon for canon, src in CANON.items() if src in out.columns and canon not in out.columns}
    out = out.rename(columns=rename_map)

    # ensure canonical columns exist (even if missing in source)
    for canon in CANON.keys():
        if canon not in out.columns:
            out[canon] = pd.NA

    # derived amounts
    if "GROSS_SALES" not in out.columns:
        out["GROSS_SALES"] = pd.to_numeric(out["VOLUME"], errors="coerce") * pd.to_numeric(out["UNIT_PRICE"], errors="coerce")
    if "NET_SALES" not in out.columns:
        out["NET_SALES"] = out["GROSS_SALES"]
    if "TOTAL_INVOICE" not in out.columns and "INVOICE_NO" in out.columns:
        out["TOTAL_INVOICE"] = out.groupby("INVOICE_NO")["NET_SALES"].transform("sum")

    return out
