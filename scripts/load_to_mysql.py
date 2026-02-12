"""
Load Processed Data to MySQL
Loads cleaned CSV files into MySQL database tables
"""

import sys
import pandas as pd
from pathlib import Path
from sqlalchemy import text
from config import get_engine

repo_root = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(repo_root))

print("Starting data load to MySQL...")

def require_file(path: Path) -> None:
    """Stop immediately if a required file is missing."""
    if not path.exists():
        raise FileNotFoundError(f"Required file not found: {path}")


def clear_tables(conn) -> None:
    """
    Remove existing rows without dropping tables.
    This function disables foreign key checks to allow truncation
    of child tables before parent tables.
    """
    conn.execute(text("SET FOREIGN_KEY_CHECKS = 0;"))

    # child tables first
    conn.execute(text("DELETE FROM raw_ndc_packaging;"))
    conn.execute(text("DELETE FROM shortage_contacts;"))

    # parent tables next
    conn.execute(text("DELETE FROM raw_ndc;"))
    conn.execute(text("DELETE FROM raw_drug_shortages;"))

    conn.execute(text("SET FOREIGN_KEY_CHECKS = 1;"))


def load_csv(conn, csv_path: Path, table_name: str) -> int:
    """Read a CSV and append rows into an existing MySQL table."""
    df = pd.read_csv(csv_path)
    df.to_sql(
        name=table_name,
        con=conn,
        if_exists="append",   # important: do not drop tables
        index=False,
        chunksize=2000,
        method="multi",
    )
    return len(df)

def load_shortage_contacts(conn, csv_path: Path) -> int:
    """
    Convert package_ndc in CSV -> shortage_id in DB, then insert.
    Supports:
      - long format: package_ndc + contact_info
      - wide format: package_ndc + contact_info_* (many columns)
    Assumes shortage_contacts table has columns: shortage_id, contact_info
    """
    df = pd.read_csv(csv_path)

    if df.empty:
        return 0

    if "package_ndc" not in df.columns:
        raise ValueError(f"{csv_path} must contain 'package_ndc'. Found: {list(df.columns)}")

    # Identify contact columns
    contact_cols = [c for c in df.columns if c == "contact_info" or c.startswith("contact_info")]
    if not contact_cols:
        raise ValueError(
            f"{csv_path} must contain 'contact_info' or 'contact_info*' columns. Found: {list(df.columns)}"
        )

    # Normalize package_ndc
    df["package_ndc"] = df["package_ndc"].astype(str).str.strip()

    # Convert to long format if needed
    if "contact_info" in df.columns and len(contact_cols) == 1:
        contacts_long = df[["package_ndc", "contact_info"]].copy()
    else:
        contacts_long = df.melt(
            id_vars=["package_ndc"],
            value_vars=contact_cols,
            var_name="contact_field",
            value_name="contact_info",
        )[["package_ndc", "contact_info"]]

    # Clean contact_info
    contacts_long["contact_info"] = contacts_long["contact_info"].astype(str).str.strip()
    contacts_long = contacts_long[contacts_long["contact_info"].notna()]
    contacts_long = contacts_long[contacts_long["contact_info"] != ""]
    contacts_long = contacts_long[contacts_long["contact_info"].str.lower() != "nan"]

    if contacts_long.empty:
        return 0

    # Pull mapping from DB: package_ndc -> one shortage_id
    mapping_df = pd.read_sql(
        """
        SELECT package_ndc, MIN(shortage_id) AS shortage_id
        FROM raw_drug_shortages
        WHERE package_ndc IS NOT NULL AND package_ndc <> ''
        GROUP BY package_ndc
        """,
        con=conn,
    )
    mapping_df["package_ndc"] = mapping_df["package_ndc"].astype(str).str.strip()

    # Merge to get shortage_id
    merged = contacts_long.merge(mapping_df, on="package_ndc", how="left")
    merged = merged.dropna(subset=["shortage_id"])

    # Build final dataframe matching the DB table
    out = merged[["shortage_id", "contact_info"]].copy()
    out["shortage_id"] = out["shortage_id"].astype(int)

    out.to_sql(
        name="shortage_contacts",
        con=conn,
        if_exists="append",
        index=False,
        chunksize=2000,
        method="multi",
    )

    return len(out)


def main() -> None:
    print("Starting data load to MySQL (pipeline-safe)...")

    # Anchor paths to repo root (so this works from anywhere)
    repo_root = Path(__file__).resolve().parents[1]
    data_dir = repo_root / "data"

    # These files must exist by the time this script runs
    csv_plan = [
        (data_dir / "ndc_core.csv", "raw_ndc"),
        (data_dir / "ndc_packaging.csv", "raw_ndc_packaging"),
        (data_dir / "drug_shortages_core.csv", "raw_drug_shortages"),
        
    ]

    # Contacts require mapping (package_ndc -> shortage_id)
    contacts_csv = data_dir / "shortage_contacts.csv"
    for csv_path, _ in csv_plan:
        require_file(csv_path)
    require_file(contacts_csv)    

    engine = get_engine()

    try:
        with engine.begin() as conn:
            print(" Connected. Clearing existing rows...")
            clear_tables(conn)
            print(" Tables cleared.")

            print("\nLoading CSV files into MySQL tables...")
            for csv_path, table_name in csv_plan:
                print(f" Loading {csv_path.name} -> {table_name}...")
                rows = load_csv(conn, csv_path, table_name)
                print(f" Inserted {rows:,} rows into {table_name}")
            print(f" Loading {contacts_csv.name} -> shortage_contacts (mapping shortage_id)...")
            rows = load_shortage_contacts(conn, contacts_csv)
            print(f" Inserted {rows:,} rows into shortage_contacts")    

        # verification
        with engine.connect() as conn:
            print("\nRow count verification:")
            for _, table_name in csv_plan:
                cnt = conn.execute(text(f"SELECT COUNT(*) FROM {table_name};")).scalar()
                print(f"  {table_name}: {int(cnt):,} rows")
            cnt = conn.execute(text("SELECT COUNT(*) FROM shortage_contacts;")).scalar()
            print(f"  shortage_contacts: {int(cnt):,} rows")
        print("\n Data load completed successfully.")

    finally:
        engine.dispose()
        print("Database connection closed.")


if __name__ == "__main__":
    main()