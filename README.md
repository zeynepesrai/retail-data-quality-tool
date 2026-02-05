# Retail Data Quality (DQ) Toolkit 🧪📊

Production-style **data quality checks** for invoice-line retail data, with **Excel reporting** and a simple CLI.

This repo is designed as a **public portfolio project**:
- Works with a well-known public dataset (Kaggle: *Online Retail*)
- Uses a **generic canonical schema** (no company tables, no internal naming)
- Includes CI + tests so it looks “real” on GitHub

---

## Demo (what you get)

After running, you'll have:

- `outputs/DQ_Report.xlsx`
  - `Summary_By_Case`
  - `Summary_By_Column`
  - `Summary_By_Reason`
  - `High_Issues_Detail`
  - `Row_Issues_Detail`
  - `Grouped_Issues`
- `outputs/labeled_rows.csv` (original rows + `dq_flag` + `dq_rule_list`)

> Tip: Add screenshots under `docs/screenshots/` and commit them so the README looks great.

Placeholders:
- `docs/screenshots/report_summary.png`
- `docs/screenshots/high_issues.png`

---

## Why this matters

Real-world analytics pipelines fail for predictable reasons:
- missing keys (InvoiceNo, dates, quantities)
- inconsistent mappings (InvoiceNo → multiple Country / CustomerID)
- invalid numeric formats
- sign errors (negative price/qty)
- duplicated invoice lines

This toolkit provides a clean pattern to **detect + label + report** these issues.

---

## Quickstart (runs without Kaggle)

```bash
python -m venv .venv
# Windows: .venv\Scripts\activate
source .venv/bin/activate

pip install -r requirements.txt

# create a small synthetic dataset (so you can run immediately)
python scripts/make_sample_data.py

# run DQ
python scripts/run_dq.py --input data/sample/online_retail_sample.csv --outdir outputs
```

---

## Using the Kaggle "Online Retail" dataset

Dataset (Kaggle): **Online Retail**  
Columns: InvoiceNo, StockCode, Description, Quantity, InvoiceDate, UnitPrice, CustomerID, Country.

1) Download the CSV from Kaggle and place it here:

```
data/raw/online_retail.csv
```

2) Run:

```bash
python scripts/run_dq.py --input data/raw/online_retail.csv --outdir outputs
```

### Optional: download via Kaggle API

If you have `kaggle.json` configured:

```bash
kaggle datasets download -d tunguz/online-retail -p data/raw --unzip
```

Then point `--input` to the extracted CSV file.

---

## DQ rules (high level)

Row-level:
- **DQ_01** completeness (mandatory fields)
- **DQ_02** Invoice date parseability
- **DQ_03** Invoice date in the future (relative to max date in data)
- **DQ_06** numeric parseability (qty, price, amounts)
- **DQ_04** negative values & sign mismatches
- **DQ_07** volume vs net sales mismatch (~0 inconsistencies)
- **DQ_05** duplicate invoice line signature
- **DQ_10** InvoiceNo maps to multiple Client/Country

Grouped:
- **DQ_12G** CustomerID maps to multiple Country
- **DQ_13G** SKU maps to multiple Description values

---

## Project structure

```
src/retail_dq/   core package
scripts/         CLI entry scripts
docs/            docs + screenshots
data/            (ignored) raw/sample data location
outputs/         reports (ignored)
tests/           small smoke tests
```

---

## Contributing / extending

Add new rules in `src/retail_dq/rules.py` and register them in `src/retail_dq/runner.py`.

---

## License

MIT
