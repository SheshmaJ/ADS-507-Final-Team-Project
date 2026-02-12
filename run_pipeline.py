#this script will run full FDA ETL pipeline using Github Actions
#The order follwed for ETL steps is
#dowload data
#process data
#load to MYSQL

# run_pipeline.py
from __future__ import annotations

import cmd
import os
import subprocess
import sys
from pathlib import Path
from datetime import datetime

from dotenv import load_dotenv
load_dotenv()

REPORT_DIR = Path("monitoring/reports")
PIPELINE_LOG = REPORT_DIR / "pipeline.log"

def log_line(msg: str, mask: bool = False) -> None:
    if mask:
        msg = "[mysql command hidden]"

    timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{timestamp} UTC] {msg}"
    print(line)
    PIPELINE_LOG.parent.mkdir(parents=True, exist_ok=True)
    with PIPELINE_LOG.open("a", encoding="utf-8") as f:
        f.write(line + "\n")


def run(cmd: str) -> None:
    is_mysql = cmd.strip().startswith("mysql")
    log_line(f"RUN: {cmd}", mask=is_mysql)
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)

    # write stdout/stderr into log file
    if result.stdout:
        with PIPELINE_LOG.open("a", encoding="utf-8") as f:
            f.write(result.stdout + "\n")
    if result.stderr:
        with PIPELINE_LOG.open("a", encoding="utf-8") as f:
            f.write(result.stderr + "\n")

    if result.returncode != 0:
        log_line(f"FAILED (exit {result.returncode}): {cmd}", mask=is_mysql)
        raise subprocess.CalledProcessError(result.returncode, cmd)


def main() -> None:
    REPORT_DIR.mkdir(parents=True, exist_ok=True)

    # reset log each run
    PIPELINE_LOG.write_text("", encoding="utf-8")
    log_line("Starting FDA ETL pipeline")

    try:
        run("python -m scripts.download_data")
        run("python -m scripts.process_data")
        run("python -m scripts.load_to_mysql")

        # SQL transformations. Read in from the .env for connection details (instead of hardcoding in the SQL file)
        port = os.getenv("DB_PORT")
        user = os.getenv("DB_USER")
        pwd = os.getenv("DB_PASSWORD")
        host = os.getenv("DB_HOST")
        db = os.getenv("DB_NAME")

        run(f'mysql -h "{host}" -P "{port}" -u "{user}" -p"{pwd}" "{db}" < sql/02_transformations.sql')
    
        log_line("ETL pipeline completed successfully.")

    except subprocess.CalledProcessError:
        log_line("ETL pipeline failed.")
        sys.exit(1)


if __name__ == "__main__":
    main()
