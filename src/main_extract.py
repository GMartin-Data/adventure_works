import multiprocessing
import sys
from datetime import datetime

from utils import create_logger, get_env, init_project


def run_extraction(script_name):
    try:
        __import__(script_name)
    except Exception as e:
        logger.error(f"❌ Error running {script_name}: {str(e)}")
        return False
    return True


if __name__ == "__main__":
    init_project()

    LOGS_DIR = get_env("LOGS_DIR")
    logger = create_logger(__name__, f"{LOGS_DIR}/main_extract.log")

    logger.info(f"🚀 Starting extraction process at {datetime.now()}")

    # List of extraction scripts to run
    extraction_scripts = [
        "extract_from_db",
        "process_parquet_files",
        "extract_from_datalake",
    ]

    # Create processes
    processes = []
    for script in extraction_scripts:
        p = multiprocessing.Process(target=run_extraction, args=(script,))
        processes.append(p)
        p.start()
        logger.info(f"Started process for {script}")

    # Wait for all processes to complete
    exit_codes = []
    for p in processes:
        p.join()
        exit_codes.append(p.exitcode == 0)

    # Check if all processes succeeded
    if all(exit_codes):
        logger.info("✅ All extractions completed successfully")
        sys.exit(0)
    else:
        logger.error("❌ Some extractions failed")
        sys.exit(1)
