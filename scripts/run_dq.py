from __future__ import annotations
import argparse
from retail_dq.runner import run_and_export

def main():
    p = argparse.ArgumentParser(description="Run invoice-line data quality checks and export an Excel report.")
    p.add_argument("--input", required=True, help="Path to input CSV (e.g., Online Retail dataset).")
    p.add_argument("--outdir", default="outputs", help="Output directory.")
    p.add_argument("--sample-rows", type=int, default=None, help="Optional: only process first N rows.")
    args = p.parse_args()

    excel_path = run_and_export(args.input, outdir=args.outdir, sample_rows=args.sample_rows)
    print(f"✅ Report written: {excel_path}")

if __name__ == "__main__":
    main()
