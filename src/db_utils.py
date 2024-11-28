import os

import pandas as pd
import pyodbc


def connect_to_sql_server(
    logger, server: str, database: str, login: str, password: str
) -> pyodbc.Connection:
    try:
        logger.info(f"Connection to server: {server}, database: {database}")

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
        logger.error(f"⚠️ Connection Error to SQL server: {e}")
        raise


def get_tables_names(logger, connection: pyodbc.Connection) -> list[str]:
    """Get all the tables names from the target database."""
    query = """
    SELECT TABLE_NAME, TABLE_SCHEMA
      FROM INFORMATION_SCHEMA.TABLES
     WHERE TABLE_SCHEMA IN (?, ?, ?)
       AND TABLE_NAME NOT LIKE 'v%'
    """

    try:
        # Squeeze is here to transform the DataFrame in a Series
        df = pd.read_sql_query(
            query, connection, params=["Person", "Production", "Sales"]
        )
        logger.info(f"👉 Got {len(df)} table names")
        df["full_name"] = df.TABLE_SCHEMA + "." + df.TABLE_NAME
        return df.full_name.to_list()
    except Exception as e:
        logger.error(f"❌ An error occured during list extraction: {e}")


def get_table_data(logger, connection: pyodbc.Connection, table_name: str) -> None:
    """Extract all data from an Azure database's given table thanks to a SQL query.

    Args:
        connection: a pyodbc connection
        table_name (str): the target table's name

    Returns:
        None, as it outputs the data in a CSV file, within the data/db folder.
    """
    os.makedirs("data/db", exist_ok=True)
    output_file = f"data/db/{table_name}.csv"
    # Review alternatives to f-strings
    query = f"SELECT * FROM {table_name}"

    try:
        df = pd.read_sql_query(
            query,
            connection
        )
        logger.info(f"✅ Successfully extracted data from table '{table_name}'")
        df.to_csv(output_file, index=False)
    except Exception as e:
        logger.error(
            f"❌ An error occured during data extractionfrom table '{table_name}':\n{e}"
        )
