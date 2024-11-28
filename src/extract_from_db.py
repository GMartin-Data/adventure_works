import warnings

from tqdm import tqdm

from db_utils import connect_to_sql_server, get_tables_names, get_table_data
from utils import create_logger, get_env, init_project


init_project()  # Should be removed when not testing

LOGS_DIR = get_env("LOGS_DIR")
logger = create_logger(__name__, f"{LOGS_DIR}/extract_from_db.log")

warnings.filterwarnings("ignore")

SQL_SERVER = get_env("SQL_SERVER")
SQL_DB = get_env("SQL_DB")
SQL_ID = get_env("SQL_ID")
SQL_PW = get_env("SQL_PW")

logger.info(f"Connexion on SERVER={SQL_SERVER}, DATABASE={SQL_DB}")

conn = connect_to_sql_server(logger, SQL_SERVER, SQL_DB, SQL_ID, SQL_PW)

tables_names = get_tables_names(logger, conn)

for table_name in tqdm(tables_names, desc="Processing Tables"):
    get_table_data(logger, conn, table_name)

conn.close()

warnings.resetwarnings()
