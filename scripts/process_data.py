"""
FDA Data Processing Script
Cleans and normalizes the downloaded FDA datasets into structured CSV tables
"""

import pandas as pd
import json
import os

print("Starting data processing...")

# ============================================
# Process NDC Dataset
# ============================================
print("\n1. Processing NDC dataset...")

try:
    # Load the NDC JSON file
    with open('data/drug-ndc-0001-of-0001.json', 'r',encoding="utf-8") as f:
        ndc_data = json.load(f)
    
    # Extract results into DataFrame
    df_ndc = pd.DataFrame(ndc_data['results'])
    print(f"   Loaded {len(df_ndc)} NDC records")
    
    # Create core NDC products table
    ndc_core_columns = [
        'product_ndc', 'generic_name', 'labeler_name', 'brand_name',
        'finished', 'marketing_category', 'dosage_form', 'route',
        'product_type', 'marketing_start_date', 'application_number'
    ]
    
    # Only keep columns that exist
    available_columns = [col for col in ndc_core_columns if col in df_ndc.columns]
    ndc_core = df_ndc[available_columns].copy()

    #prevent primary key duplicate insertion
    if "product_ndc" in ndc_core.columns:
        before = len(ndc_core)

        ndc_core = ndc_core.dropna(subset=["product_ndc"])
        ndc_core["product_ndc"] = ndc_core["product_ndc"].astype(str).str.strip()

        ndc_core = ndc_core[ndc_core["product_ndc"].ne("")]
        ndc_core = ndc_core[ndc_core["product_ndc"].str.lower().ne("nan")]
        ndc_core = ndc_core.drop_duplicates(subset=["product_ndc"], keep="first")

        after = len(ndc_core)
        print(f" Removed {before - after} duplicate/blank product_ndc rows")
    # Save core NDC table
    ndc_core.to_csv('data/ndc_core.csv', index=False)
    
    print(f"   ✓ Created ndc_core.csv ({len(ndc_core)} rows)")
    
    # Extract packaging information (one-to-many relationship)
    packaging_records = []
    for _, row in df_ndc.iterrows():
        product_ndc = row.get('product_ndc')
        
        #keep product_ndc clean
        if product_ndc is not None:
            product_ndc = str(product_ndc).strip()

        packaging_list = row.get("packaging", [])
        if isinstance(packaging_list, list):
            for pkg in packaging_list:
                packaging_records.append({
                    'product_ndc': product_ndc,
                    'package_ndc': pkg.get('package_ndc'),
                    'description': pkg.get('description'),
                    'marketing_start_date': pkg.get('marketing_start_date')
                })
    
    ndc_packaging = pd.DataFrame(packaging_records)
    if "package_ndc" in ndc_packaging.columns:
        before = len(ndc_packaging)

        ndc_packaging = ndc_packaging.dropna(subset=["package_ndc"])
        ndc_packaging["package_ndc"] = ndc_packaging["package_ndc"].fillna("").astype(str).str.strip()
        ndc_packaging = ndc_packaging[ndc_packaging["package_ndc"].ne("")]
        ndc_packaging = ndc_packaging[ndc_packaging["package_ndc"].str.lower().ne("nan")]
        ndc_packaging = ndc_packaging.drop_duplicates(subset=["package_ndc"], keep="first")

        after = len(ndc_packaging)
        print(f"Removed {before - after} duplicate/blank package_ndc rows")

    if "product_ndc" in ndc_packaging.columns:
        ndc_packaging["product_ndc"] = ndc_packaging["product_ndc"].astype(str).str.strip()


    ndc_packaging.to_csv('data/ndc_packaging.csv', index=False)
    print(f"   ✓ Created ndc_packaging.csv ({len(ndc_packaging)} packages)")
    
except Exception as e:
    print(f"   ✗ Error processing NDC dataset: {e}")

# ============================================
# Process Drug Shortages Dataset
# ============================================
print("\n2. Processing Drug Shortages dataset...")

try:
    # Load the drug shortage JSON file
    with open('data/drug-shortages-0001-of-0001.json', 'r',encoding="utf-8") as f:
        shortage_data = json.load(f)
    
    # Extract results into DataFrame
    df_shortages = pd.DataFrame(shortage_data['results'])
    print(f"   Loaded {len(df_shortages)} shortage records")
    
    # Create core shortage table with fields that actually exist
    shortage_core = pd.DataFrame({
        'package_ndc': df_shortages.get('package_ndc'),
        'generic_name': df_shortages.get('generic_name'),
        'company_name': df_shortages.get('company_name'),
        'status': df_shortages.get('status'),
        'therapeutic_category': df_shortages.get('therapeutic_category'),
        'initial_posting_date': df_shortages.get('initial_posting_date'),
        'update_date': df_shortages.get('update_date'),
        'dosage_form': df_shortages.get('presentation'),  # Use presentation field
        'reason': None  # Not available in FDA data
    })
    if "package_ndc" in shortage_core.columns:
        shortage_core = shortage_core.dropna(subset=["package_ndc"])
        shortage_core["package_ndc"] = shortage_core["package_ndc"].astype(str).str.strip()
        shortage_core = shortage_core[shortage_core["package_ndc"].ne("")]
        shortage_core = shortage_core[shortage_core["package_ndc"].str.lower().ne("nan")]
    # Save core shortage table
    shortage_core.to_csv('data/drug_shortages_core.csv', index=False)
    
    print(f"   ✓ Created drug_shortages_core.csv ({len(shortage_core)} shortages)")
    
    # Extract contact information
    contact_records = []
    for _, row in df_shortages.iterrows():
        package_ndc = row.get('package_ndc')
        if package_ndc is not None:
            package_ndc = str(package_ndc).strip()

        contact_info = row.get('contact_info')
        if contact_info:
            contact_records.append({
                'package_ndc': package_ndc,
                'contact_info': str(contact_info)
            })
    
    shortage_contacts = pd.DataFrame(contact_records, columns=["package_ndc", "contact_info"])
    shortage_contacts.to_csv("data/shortage_contacts.csv", index=False)
    if not shortage_contacts.empty:
        shortage_contacts = shortage_contacts.dropna(subset=["package_ndc"])
        shortage_contacts["package_ndc"] = shortage_contacts["package_ndc"].astype(str).str.strip()
        shortage_contacts = shortage_contacts[shortage_contacts["package_ndc"].ne("")]
        shortage_contacts = shortage_contacts[shortage_contacts["package_ndc"].str.lower().ne("nan")]
        shortage_contacts = shortage_contacts.drop_duplicates(subset=["package_ndc", "contact_info"], keep="first")
    print(f"  Created shortage_contacts.csv ({len(shortage_contacts)} contacts)")

except Exception as e:
    print(f"   ✗ Error processing Drug Shortages dataset: {e}")

print("\n✓ Data processing complete!")
print("\nGenerated files in data/ directory:")
print("  - ndc_core.csv")
print("  - ndc_packaging.csv")
print("  - drug_shortages_core.csv")
print("  - shortage_contacts.csv")
print("\nNext step: Load these CSV files into MySQL")
