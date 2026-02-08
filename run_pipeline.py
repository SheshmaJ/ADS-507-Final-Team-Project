#this script will run full FDA ETL pipeline using Github Actions
#The order follwed for ETL steps is
#dowload data
#process data
#load to MYSQL

# run_pipeline.py
from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path
from datetime import datetime

REPORT_DIR = Path("monitoring/reports")
PIPELINE_LOG = REPORT_DIR / "pipeline.log"


def log_line(msg: str) -> None:
    timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{timestamp} UTC] {msg}"
    print(line)
    PIPELINE_LOG.parent.mkdir(parents=True, exist_ok=True)
    with PIPELINE_LOG.open("a", encoding="utf-8") as f:
        f.write(line + "\n")


def run(cmd: str) -> None:
    log_line(f"RUN: {cmd}")
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)

    # write stdout/stderr into log file
    if result.stdout:
        with PIPELINE_LOG.open("a", encoding="utf-8") as f:
            f.write(result.stdout + "\n")
    if result.stderr:
        with PIPELINE_LOG.open("a", encoding="utf-8") as f:
            f.write(result.stderr + "\n")

    if result.returncode != 0:
        log_line(f"FAILED (exit {result.returncode}): {cmd}")
        raise subprocess.CalledProcessError(result.returncode, cmd)


def main() -> None:
    REPORT_DIR.mkdir(parents=True, exist_ok=True)

    # reset log each run
    PIPELINE_LOG.write_text("", encoding="utf-8")
    log_line("Starting FDA ETL pipeline")

    try:
        run("python scripts/download_data.py")
        run("python scripts/process_data.py")
        run("python scripts/load_to_mysql.py")

        # SQL transformations
        host = os.environ["DB_HOST"]
        port = os.environ["DB_PORT"]
        user = os.environ["DB_USER"]
        pwd = os.environ["DB_PASSWORD"]
        db = os.environ["DB_NAME"]

        run(
            f'mysql -h "{host}" -P "{port}" -u "{user}" -p"{pwd}" "{db}" < sql/02_transformations.sql'
        )

        log_line("ETL pipeline completed successfully.")

    except subprocess.CalledProcessError:
        log_line("ETL pipeline failed.")
        sys.exit(1)


if __name__ == "__main__":
    main()
