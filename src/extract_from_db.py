import os
from typing import List
import warnings

from dotenv import load_dotenv
from tqdm import tqdm
import pandas as pd
import pyodbc

from utils import create_logger, get_env, init_project


init_project()  # Should be removed when not testing

LOGS_DIR = get_env("LOGS_DIR")
logger = create_logger(__name__, f"{LOGS_DIR}/datalake_db_extraction.log")


def connect_to_sql_server(server, database, login, password):
    try:
        logger.info(f"Connexion au serveur : {server}, base de données : {database}")

        connection = pyodbc.connect(
            "Driver={ODBC Driver 18 for SQL Server};"
            f"Server=tcp:{server}.database.windows.net,1433;"
            f"Database={database};"
            f"UID={login};PWD={password};"
            "Encrypt=yes;TrustServerCertificate=no;"
            "Connection Timeout=30;"
        )
        logger.info("👍 Successfully connected to SQL server!")
        return connection
    except Exception as e:
        logger.error("⚠️ Connection Error to SQL server: {e}")
        raise


def get_tables_names(connection):
    query = f"""
    SELECT TABLE_NAME, TABLE_SCHEMA
      FROM INFORMATION_SCHEMA.TABLES
     WHERE TABLE_SCHEMA IN ('Person', 'Production', 'Sales')
       AND TABLE_NAME NOT LIKE 'v%'
    """

    try:
        # Squeeze is here to transform the DataFrame in a Series
        df = pd.read_sql_query(query, connection)
        logger.info(f"Got {len(df)} table names")
        df["full_name"] = df.TABLE_SCHEMA + "." + df.TABLE_NAME
        return df.full_name.to_list()
    except Exception as e:
        logger.error(f"An error occured during list extraction: {e}")


def get_table_data(connection, table_name: str) -> None:
    """
    Extract data from Azure database thanks to a SQL query.
    Output a CSV File.
    """
    os.makedirs("data/db", exist_ok=True)
    output_file = f"data/db/{table_name}.csv"
    # Review alternatives to f-strings
    query = f"SELECT * FROM {table_name}"

    try:
        df = pd.read_sql_query(query, connection)
        logger.info(f"✅ Successfully extracted data from table '{table_name}'")
        df.to_csv(output_file, index=False)
    except Exception as e:
        logger.error(
            f"❌ An error occured during data extractionfrom table '{table_name}':\n{e}"
        )


if __name__ == "__main__":
    warnings.filterwarnings("ignore")

    SQL_SERVER = get_env("SQL_SERVER")
    SQL_DB = get_env("SQL_DB")
    SQL_ID = get_env("SQL_ID")
    SQL_PW = get_env("SQL_PW")

    logger.info("Connexion on SERVER={SQL_SERVER}, DATABASE={SQL_DB}")

    conn = connect_to_sql_server(SQL_SERVER, SQL_DB, SQL_ID, SQL_PW)

    tables_names = get_tables_names(conn)

    for table_name in tqdm(tables_names, desc="Processing Tables"):
        get_table_data(conn, table_name)

    conn.close()

    warnings.resetwarnings()
