from __future__ import annotations
import os
import pandas as pd

def build_summaries(dq_row_issues: pd.DataFrame) -> dict[str, pd.DataFrame]:
    if dq_row_issues is None or dq_row_issues.empty:
        return {
            "summary_by_case": pd.DataFrame(),
            "summary_by_reason": pd.DataFrame(),
            "summary_by_column": pd.DataFrame(),
            "high_issues": pd.DataFrame(),
        }

    summary_by_case = (
        dq_row_issues.groupby(["dq_rule_id","severity"])
        .size()
        .reset_index(name="issue_count")
        .sort_values(["severity","issue_count"], ascending=[True, False])
    )
    summary_by_column = (
        dq_row_issues.groupby(["dq_rule_id","column_name","severity"])
        .size()
        .reset_index(name="issue_count")
        .sort_values("issue_count", ascending=False)
    )
    summary_by_reason = (
        dq_row_issues.groupby(["dq_rule_id","fail_reason","severity"])
        .size()
        .reset_index(name="issue_count")
        .sort_values("issue_count", ascending=False)
    )
    high_issues = dq_row_issues[dq_row_issues["severity"] == "high"].copy()
    return {
        "summary_by_case": summary_by_case,
        "summary_by_reason": summary_by_reason,
        "summary_by_column": summary_by_column,
        "high_issues": high_issues,
    }

def export_excel(dq_row_issues: pd.DataFrame, dq_group_issues: pd.DataFrame, output_path: str) -> None:
    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
    sums = build_summaries(dq_row_issues)
    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        sums["summary_by_case"].to_excel(writer, sheet_name="Summary_By_Case", index=False)
        sums["summary_by_column"].to_excel(writer, sheet_name="Summary_By_Column", index=False)
        sums["summary_by_reason"].to_excel(writer, sheet_name="Summary_By_Reason", index=False)
        sums["high_issues"].to_excel(writer, sheet_name="High_Issues_Detail", index=False)
        dq_row_issues.to_excel(writer, sheet_name="Row_Issues_Detail", index=False)
        dq_group_issues.to_excel(writer, sheet_name="Grouped_Issues", index=False)
