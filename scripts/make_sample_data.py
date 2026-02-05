from __future__ import annotations
import os
import numpy as np
import pandas as pd

def main():
    np.random.seed(7)
    n = 800

    invoice_nos = np.random.choice([f"{x:06d}" for x in range(536365, 536800)], size=n)
    stock_codes = np.random.choice(["85123A","71053","84406B","84029G","85099B"], size=n)
    desc_map = {
        "85123A":"WHITE HANGING HEART T-LIGHT HOLDER",
        "71053":"WHITE METAL LANTERN",
        "84406B":"CREAM CUPID HEARTS COAT HANGER",
        "84029G":"KNITTED UNION FLAG HOT WATER BOTTLE",
        "85099B":"JUMBO BAG RED RETROSPOT"
    }
    descriptions = [desc_map[s] for s in stock_codes]

    # dates within a year
    base = pd.Timestamp("2011-01-01")
    invoice_dates = base + pd.to_timedelta(np.random.randint(0, 365, size=n), unit="D")

    qty = np.random.randint(-5, 20, size=n)  # include negatives (returns)
    unit_price = np.round(np.random.uniform(0.5, 10.0, size=n), 2)

    customer = np.random.choice([np.nan] + [float(x) for x in range(12345, 12420)], size=n, p=[0.08] + [0.92/75]*75)
    country = np.random.choice(["United Kingdom","Germany","France","Netherlands"], size=n, p=[0.7,0.1,0.1,0.1])

    df = pd.DataFrame({
        "InvoiceNo": invoice_nos,
        "StockCode": stock_codes,
        "Description": descriptions,
        "Quantity": qty,
        "InvoiceDate": invoice_dates.astype(str),
        "UnitPrice": unit_price,
        "CustomerID": customer,
        "Country": country,
    })

    # inject a few obvious DQ issues
    df.loc[0, "InvoiceDate"] = "not_a_date"
    df.loc[1, "UnitPrice"] = "??"
    df.loc[2, "InvoiceNo"] = None
    df.loc[3, "Quantity"] = 1
    df.loc[3, "UnitPrice"] = -3.5  # negative price
    df.loc[4, "Quantity"] = 2
    df.loc[4, "UnitPrice"] = 0
    df = pd.concat([df, df.iloc[[10]]], ignore_index=True)  # duplicate line

    outdir = "data/sample"
    os.makedirs(outdir, exist_ok=True)
    outpath = os.path.join(outdir, "online_retail_sample.csv")
    df.to_csv(outpath, index=False)
    print(f"✅ Wrote sample dataset: {outpath}")

if __name__ == "__main__":
    main()
