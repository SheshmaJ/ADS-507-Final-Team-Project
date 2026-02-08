"""
STREAMLIT DASHBOARD APPLICATION
This module initializes and runs the Streamlit dashboard application.
"""


#Our main focus is to answer
#Which manufacturers have the highest shortage risk?
#Do branded drugs have longer shortage durations than generics?
#Which package types are most vulnerable to shortages?




import os
import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text

# Page setup 
st.set_page_config(page_title="FDA Shortage Dashboard", layout="wide")
st.title("FDA Drug Shortage Monitoring Dashboard")
st.caption("Interactive dashboard for FDA drug shortages")

DB_USER = os.getenv("DB_USER", "root")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST", "127.0.0.1")
DB_PORT = os.getenv("DB_PORT", "3306")
DB_NAME = os.getenv("DB_NAME", "fda_shortage_db")

if not DB_PASSWORD:
    st.error("DB_PASSWORD is not set")
    st.stop()

@st.cache_resource
def get_engine():
    return create_engine(
        f"mysql+mysqlconnector://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}",
        pool_pre_ping=True,
    )

engine = get_engine()

def q(sql, params=None):
    return pd.read_sql(text(sql), engine, params=params or {})


# Sidebar filters

st.sidebar.header("Filters")
only_current = st.sidebar.checkbox("Only current shortages", True)

@st.cache_data
def filter_lists():
    return (
        q("SELECT DISTINCT company_name FROM shortages_with_ndc WHERE company_name IS NOT NULL ORDER BY 1")["company_name"].tolist(),
        q("SELECT DISTINCT route FROM shortages_with_ndc WHERE route IS NOT NULL ORDER BY 1")["route"].tolist(),
        q("SELECT DISTINCT shortage_dosage_form FROM shortages_with_ndc WHERE shortage_dosage_form IS NOT NULL ORDER BY 1")["shortage_dosage_form"].tolist(),
        q("SELECT DISTINCT therapeutic_category FROM shortages_with_ndc WHERE therapeutic_category IS NOT NULL ORDER BY 1")["therapeutic_category"].tolist(),
    )

manufacturers, routes, dosages, therapeutics = filter_lists()

mfg = st.sidebar.selectbox("Manufacturer", ["All"] + manufacturers)
route = st.sidebar.selectbox("Route", ["All"] + routes)
dose = st.sidebar.selectbox("Dosage form", ["All"] + dosages)
ther = st.sidebar.selectbox("Therapeutic category", ["All"] + therapeutics)
limit_rows = st.sidebar.slider("Detail rows", 50, 1000, 200, 50)

filters, params = [], {}
if only_current:
    filters.append("status='Current'")
if mfg != "All":
    filters.append("company_name=:mfg")
    params["mfg"] = mfg
if route != "All":
    filters.append("route=:route")
    params["route"] = route
if dose != "All":
    filters.append("shortage_dosage_form=:dose")
    params["dose"] = dose
if ther != "All":
    filters.append("therapeutic_category=:ther")
    params["ther"] = ther

where = "WHERE " + " AND ".join(filters) if filters else ""


# KPIs

kpi = q(
    """
    SELECT
      COUNT(*) total,
      SUM(status='Current') current,
      COUNT(DISTINCT company_name) manufacturers,
      COUNT(DISTINCT package_ndc) packages
    FROM shortages_with_ndc;
    """
).iloc[0]

c1, c2, c3, c4 = st.columns(4)
c1.metric("Total shortages", f"{int(kpi.total):,}")
c2.metric("Current shortages", f"{int(kpi.current):,}")
c3.metric("Manufacturers", f"{int(kpi.manufacturers):,}")
c4.metric("Packages affected", f"{int(kpi.packages):,}")

st.divider()


# 1) Manufacturer risk

st.subheader("1) Manufacturer Impact Analysis")

df_manu = q(
    """
    SELECT company_name,
           current_affected_packages,
           current_affected_products
    FROM current_manufacturer_risk
    ORDER BY current_affected_packages DESC
    LIMIT 25;
    """
)

c1, c2 = st.columns([2, 1])
c1.dataframe(df_manu, use_container_width=True)
c2.bar_chart(df_manu.set_index("company_name")[["current_affected_packages"]])

st.divider()


# 2) Branded vs Generic

st.subheader("2) Branded vs Generic Shortage Duration(use filter)")

df_brand = q(
    f"""
    SELECT
      CASE
        WHEN brand_name IS NOT NULL AND brand_name<>'' THEN 'Branded'
        ELSE 'Generic/Unbranded'
      END drug_type,
      COUNT(*) shortage_count,
      ROUND(AVG(DATEDIFF(CURDATE(),initial_posting_date_dt)),1) AS avg_days_active
    FROM shortages_with_ndc
    {where}
    AND initial_posting_date_dt IS NOT NULL
    GROUP BY drug_type;
    """,
    params,
)

c1, c2 = st.columns([2, 1])
c1.dataframe(df_brand, use_container_width=True)
if not df_brand.empty:
    c2.bar_chart(df_brand.set_index("drug_type")[["avg_days_active"]])

st.divider()


# 3) Package vulnerability

st.subheader("3) Packaging Types Most Affected(use filter)")

df_pkg = q(
    f"""
    SELECT
      CASE
        WHEN LOWER(package_description) LIKE '%bottle%' THEN 'Bottle'
        WHEN LOWER(package_description) LIKE '%vial%' THEN 'Vial'
        WHEN LOWER(package_description) LIKE '%blister%' THEN 'Blister'
        WHEN LOWER(package_description) LIKE '%carton%' THEN 'Carton'
        ELSE 'Other'
      END package_type,
      COUNT(*) shortage_count
    FROM shortages_with_ndc
    {where}
    AND package_description IS NOT NULL
    GROUP BY package_type
    ORDER BY shortage_count DESC;
    """,
    params,
)

c1, c2 = st.columns([2, 1])
c1.dataframe(df_pkg, use_container_width=True)
c2.bar_chart(df_pkg.set_index("package_type"))

st.divider()


# Detailes shortage details

st.subheader("Detailed Shortage Records")

mode = st.selectbox(
    "Drill by",
    ["Manufacturer", "Generic Name", "Package NDC"],
)

if mode == "Manufacturer":
    val = st.selectbox("Select", manufacturers)
    df = q(
        f"""
        SELECT * FROM shortages_with_ndc
        WHERE company_name=:v
        ORDER BY update_date DESC
        LIMIT {limit_rows};
        """,
        {"v": val},
    )

elif mode == "Generic Name":
    names = q(
        "SELECT DISTINCT ndc_generic_name FROM shortages_with_ndc WHERE ndc_generic_name IS NOT NULL ORDER BY 1"
    )["ndc_generic_name"].tolist()
    val = st.selectbox("Select", names)
    df = q(
        f"""
        SELECT * FROM shortages_with_ndc
        WHERE ndc_generic_name=:v
        ORDER BY update_date DESC
        LIMIT {limit_rows};
        """,
        {"v": val},
    )

else:
    pkgs = q(
        "SELECT DISTINCT package_ndc FROM shortages_with_ndc WHERE package_ndc IS NOT NULL ORDER BY 1"
    )["package_ndc"].tolist()
    val = st.selectbox("Select", pkgs)
    df = q(
        f"""
        SELECT * FROM shortages_with_ndc
        WHERE package_ndc=:v
        ORDER BY update_date DESC
        LIMIT {limit_rows};
        """,
        {"v": val},
    )

st.dataframe(df, use_container_width=True)

engine.dispose()
