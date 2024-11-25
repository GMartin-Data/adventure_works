import logging
import os
from typing import List
import warnings

from dotenv import load_dotenv
from tqdm import tqdm
import pandas as pd
import pyodbc

# 0. Load and source credentials
load_dotenv()

LOGS_DIRECTORY = "./logs"
if not os.path.exists(LOGS_DIRECTORY):
    os.makedirs(LOGS_DIRECTORY)

# Logger configuration
logging.basicConfig(
    filename="./logs/sql_extraction.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)


def connect_to_sql_server(server, database, login, password):
    try:
        logging.info(f"Connexion au serveur : {server}, base de données : {database}")

        connection = pyodbc.connect(
            "Driver={ODBC Driver 18 for SQL Server};"
            f"Server=tcp:{server}.database.windows.net,1433;"
            f"Database={database};"
            f"UID={login};PWD={password};"
            "Encrypt=yes;TrustServerCertificate=no;"
            "Connection Timeout=30;"
        )
        logging.info("👍 Successfully connected to SQL server!")
        return connection
    except Exception as e:
        logging.error("⚠️ Connection Error to SQL server: {e}")
        raise
        

def get_tables_names(connection):
    query = f"""
    SELECT TABLE_NAME, TABLE_SCHEMA
    FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA IN ('Person', 'Product', 'Sales')
    """
    
    try:
        # Squeeze is here to transform the DataFrame in a Series
        df = pd.read_sql_query(query, connection)
        logging.info(f"Got {len(df)} table names")
        df["full_name"] = df.TABLE_SCHEMA + "." + df.TABLE_NAME
        return df.full_name.to_list()
    except Exception as e:
        logging.error(f"An error occured during list extraction: {e}")


def get_table_data(connection, table_name: str) -> None:
    """
    Extract data from Azure database thanks to a SQL query.
    Output a CSV File.
    """
    output_file = f"data/{table_name}.csv"
    # Review alternatives to f-strings
    query = f"SELECT * FROM {table_name}"
    
    try:
        df = pd.read_sql_query(query, connection)
        logging.info(f"5 first lines of extracted data:\n {df.head()}")
        df.to_csv(output_file, index=False)
    except Exception as e:
        logging.error(f"An error occured during data extraction: {e}")


if __name__ == "__main__":
    warnings.filterwarnings("ignore")
    
    SQL_SERVER = os.getenv("SQL_SERVER")
    SQL_DB = os.getenv("SQL_DB")
    SQL_ID = os.getenv("SQL_ID")
    SQL_PW = os.getenv("SQL_PW")

    # Secure credentials are properly set
    if not all([SQL_SERVER, SQL_DB, SQL_ID, SQL_PW]):
        logging.error("⚠️ At least one environment variable missing!")
        raise ValueError("⚠️ At least one environment variable missing!")

    logging.info("Connexion on SERVER={SQL_SERVER}, DATABASE={SQL_DB}")
    
    # Secure output folder exists
    if not os.path.exists("data/"):
        os.makedirs("data/")
        logging.info("👍 Folder {output_dir} was created!")

    # Connection
    conn = connect_to_sql_server(SQL_SERVER, SQL_DB, SQL_ID, SQL_PW)

    tables_names = get_tables_names(conn)
    
    for table_name in tqdm(tables_names, desc="Processing Tables"):
        get_table_data(conn, table_name)

    conn.close()

    warnings.resetwarnings()