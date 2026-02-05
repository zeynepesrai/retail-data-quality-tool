import pandas as pd
from retail_dq.runner import run_dq

def test_run_smoke():
    df = pd.DataFrame({
        "InvoiceNo": ["1","1"],
        "StockCode": ["A","A"],
        "Description": ["X","X"],
        "Quantity": [1,1],
        "InvoiceDate": ["2011-01-01","2011-01-01"],
        "UnitPrice": [2.0,2.0],
        "CustomerID": [12345,12345],
        "Country": ["UK","UK"],
    })
    labeled, row_issues, grp_issues = run_dq(df)
    assert "dq_flag" in labeled.columns
