# Walmart-E-Commerce-Sales-Data-Pipeline
![MIT License](https://img.shields.io/badge/License-MIT-yellow.svg)

## Overview

This project implements a **Data Pipeline** for analyzing Walmart's sales data, focusing on public holidays and their impact on sales. The pipeline extracts data from two sources, performs transformation and cleaning, aggregates the data, and loads it into both CSV files and (optionally) a PostgreSQL database.

The data sources used in the pipeline are:
1. `grocery_sales.csv`: Contains weekly sales data for Walmart stores.
2. `extra_data.parquet`: Contains complementary data like holiday information, temperature, fuel price, CPI, and unemployment rate.

## Objective

The primary goal of this project is to demonstrate proficiency in **ETL** (Extract, Transform, Load) processes, particularly for data engineering roles. The pipeline processes Walmart's sales data and produces the following outputs:
1. Cleaned and transformed data saved as `clean_data.csv`.
2. Aggregated monthly sales data saved as `agg_data.csv`.

-----

## Features

1. **Extract**: Extracts and merges data from the provided CSV and Parquet files.
2. **Transform**: Cleans and transforms the data by:
   - Filling missing values.
   - Adding a 'Month' column.
   - Filtering sales greater than $10,000.
3. **Aggregation**: Calculates the average weekly sales per month.
4. **Load**: Saves the cleaned and aggregated data into CSV files and optionally to a PostgreSQL database.
5. **Validation**: Validates that the output files exist in the working directory.

## Dependencies
- `pandas`
- `sqlalchemy`
- `pytest`
- `logging`

--------

## The Code
1) `Logging Setup`: This section sets up the logging configuration for the script. It defines the logging level, which determines the severity of messages that will be captured. It also formats the log entries with timestamps and log levels, and specifies that logs will be written both to a log file ("pipeline.log") and to the console.
2) `create_sql_tables()`: This function creates two tables in a PostgreSQL database if they do not already exist. The tables are used to store cleaned sales data and monthly sales averages. The function takes a database connection URL as an argument and uses SQLAlchemy's create_engine to interact with the database.
3) `extract()`: The extract function reads data from two source files (CSV and Parquet formats), merges them on the "index" column, and returns the combined dataset. If the "index" column is missing from either file, an error is logged and raised. This function helps to gather and combine multiple data sources.
4) `transform()`: This function performs several data cleaning and transformation tasks on the raw input data. It fills missing values in certain columns with the mean of the respective column, converts a "Date" column to a datetime format, extracts the month from the "Date" column, and filters the data to include only rows where weekly sales are above a threshold. The result is a cleaned dataset ready for further processing.
5) `avg_weekly_sales_per_month()`: This function calculates the average weekly sales for each month by grouping the data by the "Month" column and calculating the mean of the "Weekly_Sales" values for each group. The result is an aggregated dataset that contains the average sales for each month.
6) `load()`: The load function takes the cleaned and aggregated data and saves it into CSV files. It also optionally loads the data into a PostgreSQL database if a database connection URL is provided. The function logs the success or failure of each file save operation.
7) `validation()`: This function checks if the CSV files created in the load function actually exist. It logs a success message if the files are found, or an error message if they are missing. This serves as a simple validation step to ensure that the pipeline has completed successfully.
8) `main()`: This function executes all the script.
```python
from sqlalchemy import create_engine
import pandas as pd
import logging
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler("pipeline.log"),  
        logging.StreamHandler()               
    ]
)


def create_sql_tables(db_url="postgresql://user:password@localhost/sales_db"):
    """Defines schema and ensures necessary tables exist in PostgreSQL."""
    engine = create_engine(db_url)
    with engine.connect() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS clean_sales (
                Store_ID INTEGER,
                Weekly_Sales FLOAT,
                IsHoliday BOOLEAN,
                CPI FLOAT,
                Unemployment FLOAT,
                Month INTEGER,
                PRIMARY KEY (Store_ID, Month)
            );
            CREATE TABLE IF NOT EXISTS monthly_sales (
                Month INTEGER PRIMARY KEY,
                Avg_Sales FLOAT
            );
        """)
    engine.dispose()


def extract(store_data, extra_data):
    """
    Extracts data from source files and combines both files.
    
    Args:
        store_data: CSV data file
        extra_data: PARQUET data file
    
    Returns:
        merged_df: Combination of both extracted files.
    """
    
    try:
        df = pd.read_csv(store_data)
        extra_df = pd.read_parquet(extra_data)

        if "index" not in df.columns or "index" not in extra_df.columns:
            logging.error("The 'index' column is missing from one of the datasets.")
            raise KeyError("The 'index' column is missing from one of the datasets.")
        
        merged_df = df.merge(extra_df, on="index")
        logging.info("Data successfully extracted and merged.")
        return merged_df

    except Exception as e:
        logging.error(f"Error in extract(): {e}")
        raise


def transform(raw_data):
    """
    Cleans and transforms the dataset:
    - Fills missing values for Weekly_Sales, CPI, and Unemployment.
    - Converts the Date column to datetime.
    - Extracts the Month for aggregation.
    
    Args:
        raw_data (pd.DataFrame): The raw input dataframe.
    
    Returns:
        clean_data (pd.DataFrame): The cleaned and transformed dataset.
    """

    try:
        raw_data.fillna({
            "Weekly_Sales": raw_data["Weekly_Sales"].mean(),
            "CPI": raw_data["CPI"].mean(),
            "Unemployment": raw_data["Unemployment"].mean()
        }, inplace=True)

        raw_data["Date"] = pd.to_datetime(raw_data["Date"], format="%Y-%m-%dT%H:%M:%S.%f", errors="coerce")
        raw_data["Month"] = raw_data["Date"].dt.month

        clean_data = raw_data.loc[
            raw_data["Weekly_Sales"] > 10000,
            ["Store_ID", "Weekly_Sales", "IsHoliday", "CPI", "Unemployment", "Month"]
        ]

        logging.info("Data transformation successful.")
        return clean_data

    except Exception as e:
        logging.error(f"Error in transform(): {e}")
        raise


def avg_weekly_sales_per_month(clean_data):
    """
    Aggregates by average sales and groups by month.
    
    Args:
        clean_data (pd.DataFrame): The cleaned and transformed dataset coming from transform().
    
    Returns:
        agg_data (pd.DataFrame): The aggregated sales grouped by month.
    """
    
    try:
        agg_data = clean_data.groupby("Month")["Weekly_Sales"].mean().reset_index()
        agg_data.rename(columns={"Weekly_Sales": "Avg_Sales"}, inplace=True)
        agg_data = agg_data.round(2)

        logging.info("Average weekly sales per month calculated successfully.")
        return agg_data

    except Exception as e:
        logging.error(f"Error in avg_weekly_sales_per_month(): {e}")
        raise


def load(data_dict, db_url=None):
    """
    Loads the data to CSV files and optionally to a PostgreSQL database.

    Args:
        data_dict: Dictionary containing the names of the files to be created as keys, 
                   and clean_data and agg_data as values.
        db_url: Database connection string (optional).
    """
    
    try:
        for name, df in data_dict.items():
            file_path = f"{name}.csv"
            df.to_csv(file_path, index=False)
            logging.info(f"{file_path} saved successfully.")

        if db_url:
            engine = create_engine(db_url)
            for name, df in data_dict.items():
                df.to_sql(name, engine, if_exists="replace", index=False)
            engine.dipose()
            logging.info("Data successfully loaded into PostgreSQL.")

    except Exception as e:
        logging.error(f"Error in load(): {e}")
        raise


def validation(val_list):
    """
    Validates load() created files.
    
    Args:
        val_list: The list containing the name of the CSV files to validate.
    """
    for file in val_list:
        if Path(file).exists():
            logging.info(f"{file} validated successfully.")
        else:
            logging.error(f"Error: {file} was not created.")


def main(file_1, file_2, db_url=None):
    """
    Executes the full data pipeline:
    - Extracts data from CSV and Parquet files.
    - Transforms the data by cleaning and filtering it.
    - Aggregates weekly sales per month.
    - Loads the cleaned and aggregated data into CSV files (and optionally into PostgreSQL).
    - Validates that the output files were successfully created.

    Args:
        file_1 (str): Path to the store sales CSV file.
        file_2 (str): Path to the extra data Parquet file.
        db_url (str, optional): Database connection string for PostgreSQL.
    """

    try:
        logging.info("Starting data pipeline execution.")
        
        merged_df = extract(file_1, file_2)
        clean_data = transform(merged_df)
        agg_data = avg_weekly_sales_per_month(clean_data)

        load({"clean_data": clean_data, "agg_data": agg_data}, db_url)

        val_list = ["clean_data.csv", "agg_data.csv"]
        validation(val_list)

        logging.info("Data pipeline execution completed successfully.\n")

    except Exception as e:
        logging.critical(f"Critical error in main(): {e}")


if __name__ == "__main__":
    main("grocery_sales.csv", "extra_data.parquet")
    
    # If real life scenario, loading into a postgre database, then the following line should be used:
    #main("grocery_sales.csv", "extra_data.parquet", "postgresql://user:password@localhost/sales_db")
```
### Output
Since logging was used in this script, when succesfully run, the following output is produced:
```bash
2025-03-07 18:50:23 - INFO - Starting data pipeline execution.
2025-03-07 18:50:23 - INFO - Data successfully extracted and merged.
2025-03-07 18:50:23 - INFO - Data transformation successful.
2025-03-07 18:50:23 - INFO - Average weekly sales per month calculated successfully.
2025-03-07 18:50:23 - INFO - clean_data.csv saved successfully.
2025-03-07 18:50:23 - INFO - agg_data.csv saved successfully.
2025-03-07 18:50:23 - INFO - clean_data.csv validated successfully.
2025-03-07 18:50:23 - INFO - agg_data.csv validated successfully.
2025-03-07 18:50:23 - INFO - Data pipeline execution completed successfully.
```

## Tests
`test_transform()`:
This test function validates the behavior of the transform function from the pipeline. It creates a sample DataFrame representing raw sales data with missing values and dates in various columns.
Key assertions:
- Checks if a new column "Month" is created after transformation.
- Verifies that there are no missing values in the "Weekly_Sales", "CPI", and "Unemployment" columns, ensuring that the missing values are filled.
- Ensures that the "Weekly_Sales" values are filtered correctly by asserting that no values below 10,000 remain after filtering.
The goal of this test is to ensure that the transformation logic (e.g., handling missing values, adding new columns, and applying filters) works as expected.

`test_avg_weekly_sales_per_month()`:
This test checks the functionality of the avg_weekly_sales_per_month function from the pipeline. It creates a sample clean dataset with sales data for different months.
Key assertions:
- Ensures that the output contains a "Month" column.
- Checks that the output also includes an "Avg_Sales" column, which is the result of calculating the average weekly sales for each month.
- Verifies that the number of unique months in the aggregated data is correct (i.e., 3 months).
- Confirms that the average weekly sales for the first month are correctly calculated to 19,000, using rounding to match the expected result.
This test verifies that the aggregation process correctly computes the average sales per month and that the resulting dataset has the correct structure and values.
```python
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
```

-----

## Conclusion
The Walmart-E-Commerce-Sales-Data-Pipeline project demonstrates an effective approach to analyzing and processing retail sales data through an ETL pipeline. By combining multiple data sources and applying necessary data transformations, this project not only cleanses and aggregates data but also provides actionable insights into how public holidays impact sales. The pipeline is structured to be scalable and modular, allowing for future extensions, such as incorporating additional data sources or integrating machine learning models to predict sales trends.

Key takeaways from the project:
- Successful implementation of ETL processes with robust logging and error handling.
- Data is cleaned and transformed to eliminate gaps, standardize formats, and ensure relevance.
- Aggregated data is stored efficiently in both CSV files and a PostgreSQL database, ensuring flexibility for further analysis.

The project serves as a valuable reference for professionals looking to build scalable data pipelines, particularly in the context of analyzing retail data. It is also a solid example of how to manage and integrate multiple data sources, apply data transformation techniques, and store the final results for further use.

---

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
