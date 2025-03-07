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