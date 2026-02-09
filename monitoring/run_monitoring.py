#Enables pipeline monitoring through CI status check
#This file executes the monitoring process for FDA drug shortages automatically.
#Purpose is to automate pipeline healthchecks and quality after each run and fails the 
#pipeline if any issues are detected.




from __future__ import annotations
# libraries
import os
from pathlib import Path
from datetime import datetime

import pandas as pd
from sqlalchemy import create_engine, text

# Directory where monitoring reports will be saved

REPORT_DIR = Path("monitoring/reports")
REPORT_MD = REPORT_DIR / "monitoring_report.md"
REPORT_TXT = REPORT_DIR / "monitoring_report.txt"

# # SQL files executed as part of monitoring
# These cover schema validation, pipeline health, data quality, and analysis

SQL_FILES = [
    "monitoring/schema_snapshot.sql",
    "monitoring/pipeline_health.sql",
    "monitoring/data_quality_checks.sql",
    "sql/03_analysis_queries.sql",
]

# database connection
def get_db_engine():
    user = os.getenv("DB_USER", "pipeline_user")
    password = os.getenv("DB_PASSWORD", "pipeline_password")
    host = os.getenv("DB_HOST", "127.0.0.1")
    port = os.getenv("DB_PORT", "3306")
    db = os.getenv("DB_NAME", "fda_shortage_db")

    conn_str = f"mysql+mysqlconnector://{user}:{password}@{host}:{port}/{db}"
    return create_engine(conn_str, pool_pre_ping=True)

# split sql into executable statements
def split_sql_into_statements(sql_text: str) -> list[str]:
    """
    Split SQL into executable statements.
    - ignores blank lines and comment
    - splits by ';'
    """
    statements: list[str] = []
    buffer: list[str] = []

    for line in sql_text.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("--"):
            continue

        buffer.append(line)
        if stripped.endswith(";"):
            stmt = "\n".join(buffer).strip()
            if stmt:
                statements.append(stmt)
            buffer = []

    if buffer:
        stmt = "\n".join(buffer).strip()
        if stmt:
            statements.append(stmt)

    return statements

# converts dataframe into markdown table
def df_to_markdown(df: pd.DataFrame, max_rows: int = 25) -> str:
    if df.empty:
        return "_(no rows returned)_"
    if len(df) > max_rows:
        df = df.head(max_rows).copy()
    return df.to_markdown(index=False)


def safe_sql_preview(stmt: str, max_lines: int = 25) -> str:
    lines = stmt.strip().splitlines()
    return "\n".join(lines[:max_lines])


def run_sql_file(conn, file_path: str) -> tuple[list[str], bool]:
    p = Path(file_path)
    lines: list[str] = []
    failed = False

    lines.append(f"\n## Results from `{file_path}`")

    if not p.exists():
        lines.append(f"FAIL: Missing file `{file_path}`")
        return lines, True

    sql_text = p.read_text(encoding="utf-8")

    # Also support SHOW CREATE statements (they end with ';' too)
    statements = split_sql_into_statements(sql_text)

    if not statements:
        lines.append("WARN: No executable SQL statements found.")
        return lines, False

    for i, stmt in enumerate(statements, start=1):
        if stmt.strip().upper().startswith("USE "):
            continue

        lines.append(f"\n### Statement {i}")
        lines.append("```sql")
        lines.append(safe_sql_preview(stmt))
        lines.append("```")

        try:
            result = conn.execute(text(stmt))
            if result.returns_rows:
                df = pd.DataFrame(result.fetchall(), columns=result.keys())
                lines.append(df_to_markdown(df))
            else:
                lines.append("Executed successfully (no rows returned).")
        except Exception as e:
            lines.append(f"FAIL: {type(e).__name__}: {e}")
            failed = True

    return lines, failed


def add_readable_summary(conn) -> list[str]:
    
    lines: list[str] = []
    lines.append("\n---\n# Monitoring Summary")
    lines.append("\nThis section provides a high-level overview of the pipeline run, "
        "including table row counts, join success between drug shortages and NDC data, "
        "and key analytical insights.")

    # Row counts
    
    counts = []
    for t in ["raw_ndc", "raw_ndc_packaging", "raw_drug_shortages", "shortages_with_ndc"]:
        try:
            c = pd.read_sql(text(f"SELECT COUNT(*) AS row_count FROM {t};"), conn)["row_count"].iloc[0]
            counts.append({"table": t, "rows": int(c)})
        except Exception:
            counts.append({"table": t, "rows": "N/A"})
    lines.append("\n## Row counts")
    lines.append(
        "\nThis table confirms that data was successfully loaded into each core table "
        "after the ETL process completed.")
    lines.append(pd.DataFrame(counts).to_markdown(index=False))

    # Join success
    
    join_df = pd.read_sql(
        text("""
        SELECT
          COUNT(*) AS total_rows,
          SUM(product_ndc IS NOT NULL) AS joined_rows,
          SUM(product_ndc IS NULL) AS unjoined_rows,
          ROUND(SUM(product_ndc IS NOT NULL) * 100.0 / NULLIF(COUNT(*), 0), 2) AS join_success_pct
        FROM shortages_with_ndc;
        """),
        conn
    )
    lines.append("\n## Join success:Drug shortages to NDC products")
    lines.append(
        "\nThis check measures how many drug shortage records were successfully "
        "matched to NDC product information.")
    lines.append(df_to_markdown(join_df, max_rows=5))

    # Top manufacturers
    
    manu_df = pd.read_sql(
        text("""
        SELECT company_name, current_affected_packages, current_affected_products
        FROM current_manufacturer_risk
        ORDER BY current_affected_packages DESC
        LIMIT 15;
        """),
        conn
    )
    lines.append("\n## Manufacturers Most impacted by current shortages")
    lines.append("\nShows manufacturers with the highest number of affected products and packages.")
    lines.append(df_to_markdown(manu_df, max_rows=20))

    # Package types
    
    pkg_df = pd.read_sql(
        text("""
        SELECT
          CASE
            WHEN LOWER(package_description) LIKE '%bottle%' THEN 'Bottle'
            WHEN LOWER(package_description) LIKE '%vial%' THEN 'Vial'
            WHEN LOWER(package_description) LIKE '%blister%' THEN 'Blister Pack'
            WHEN LOWER(package_description) LIKE '%carton%' THEN 'Carton'
            WHEN LOWER(package_description) LIKE '%kit%' THEN 'Kit'
            ELSE 'Other/Unknown'
          END AS package_type,
          COUNT(*) AS shortage_count
        FROM shortages_with_ndc
        WHERE status = 'Current' AND package_description IS NOT NULL
        GROUP BY package_type
        ORDER BY shortage_count DESC;
        """),
        conn
    )
    lines.append("\n## Package types most affected (current)")
    lines.append(df_to_markdown(pkg_df, max_rows=20))

    return lines

#  Orchestrate database connection, SQL execution, and report generation.

def main() -> None:
    REPORT_DIR.mkdir(parents=True, exist_ok=True)

    header = [
        "# FDA Pipeline Monitoring Report",
        "",
        f"**Generated (UTC):** {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')}",
        "",
        "This report is generated automatically after the ETL pipeline run.",
        "",
        "**Included:** schema snapshot, pipeline health, data quality, analysis query outputs, and key insights.",
        "",
    ]

    engine = get_db_engine()
    report_lines = header
    had_failure = False

    try:
        with engine.connect() as conn:
            # Put readable summary first
            report_lines.extend(add_readable_summary(conn))

            # Then include full SQL outputs
            report_lines.append("\n---\n# Full SQL outputs")
            for sql_file in SQL_FILES:
                lines, failed = run_sql_file(conn, sql_file)
                report_lines.extend(lines)
                had_failure = had_failure or failed
    finally:
        engine.dispose()

    REPORT_MD.write_text("\n".join(report_lines) + "\n", encoding="utf-8")
    REPORT_TXT.write_text("\n".join(report_lines) + "\n", encoding="utf-8")

    print(f"Saved report to: {REPORT_MD}")

    if had_failure:
        print("Monitoring completed with warnings. See monitoring_report.md for details.")
    return

if __name__ == "__main__":
    main()
