import pytest
import pandas as pd
from wallmart_pipeline import transform, avg_weekly_sales_per_month 

def test_transform():
    data = pd.DataFrame({
        "Store_ID": [1, 2, 3],
        "Weekly_Sales": [15000, None, 8000],
        "IsHoliday": [False, True, False],
        "CPI": [200.5, None, 190.3],
        "Unemployment": [6.5, 7.1, None],
        "Date": ["2024-01-15T00:00:00.000", "2024-02-20T00:00:00.000", "2024-03-10T00:00:00.000"]
    })
    transformed_data = transform(data)
    
    assert "Month" in transformed_data.columns, "Month column not created"
    assert transformed_data["Weekly_Sales"].isnull().sum() == 0, "Missing Weekly_Sales not filled"
    assert transformed_data["CPI"].isnull().sum() == 0, "Missing CPI not filled"
    assert transformed_data["Unemployment"].isnull().sum() == 0, "Missing Unemployment not filled"
    assert transformed_data["Weekly_Sales"].min() > 10000, "Filtering condition not applied correctly"

def test_avg_weekly_sales_per_month():
    clean_data = pd.DataFrame({
        "Month": [1, 1, 2, 2, 3, 3],
        "Weekly_Sales": [20000, 18000, 22000, 21000, 25000, 23000]
    })
    
    agg_data = avg_weekly_sales_per_month(clean_data)
    
    assert "Month" in agg_data.columns, "Month column missing in aggregated data"
    assert "Avg_Sales" in agg_data.columns, "Avg_Sales column missing"
    assert len(agg_data) == 3, "Incorrect number of months aggregated"
    assert round(agg_data.loc[agg_data["Month"] == 1, "Avg_Sales"].values[0], 2) == 19000.0, "Incorrect average calculation for month 1"


if __name__ == "__main__":
    pytest.main(["-v", "wallmart_pipeline_pytest.py"])
