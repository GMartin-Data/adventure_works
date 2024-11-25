import os

from dotenv import load_dotenv
import duckdb
import pyodbc
import polars as pl

# 0. Load and source credentials
load_dotenv()

SQL_SERVER = os.getenv("SQL_SERVER")
SQL_DB = os.getenv("SQL_DB")
SQL_ID = os.getenv("SQL_ID")
SQL_PW = os.getenv("SQL_PW")

# 1. Connect to Azure SQL database
conn_str = (
    "DRIVER={ODBC Driver 18 for SQL Server};"
    f"SERVER={SQL_SERVER}.database.windows.net;"
    f"DATABASE={SQL_DB};"
    f"UID={SQL_ID};"
    f"PWD={SQL_PW};"
)
azure_sql_conn = pyodbc.connect(conn_str)


# 2. Use DuckDB to query Azure Database
# 2.1. Establish DuckDB connection
duckdb_conn = duckdb.connect(":memory:")

# 2.2. Query Azure database via DuckDB
query = """
SELECT TOP 10 *
  FROM Production.Product
"""

# This integrates DuckDB with pyodbc to run the query
duckdb_conn.execute(f"INSTALL httpfs; LOAD httpfs;")
results = duckdb_conn.execute(
    f"SELECT * FROM read_odbc('{conn_str}', '{query}')"
).fetch_arrow_table()


# 3. Convert results to Polars DataFrame
polars_df = pl.from_arrow(results)

# 4. Export CSV
polars_df.write_csv("data/test.csv")
