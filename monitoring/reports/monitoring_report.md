# FDA Pipeline Monitoring Report

**Generated (UTC):** 2026-02-09 01:32:59

This report is generated automatically after the ETL pipeline run.

**Included:** schema snapshot, pipeline health, data quality, analysis query outputs, and key insights.


---
# Summary (easy to read)

## Row counts
| table              |   rows |
|:-------------------|-------:|
| raw_ndc            | 128780 |
| raw_ndc_packaging  | 244838 |
| raw_drug_shortages |   1750 |
| shortages_with_ndc |   1750 |

## Join success (shortages → NDC)
|   total_rows |   joined_rows |   unjoined_rows |   join_success_pct |
|-------------:|--------------:|----------------:|-------------------:|
|         1750 |          1578 |             172 |              90.17 |

## Top manufacturers impacted (current shortages)
| company_name                        |   current_affected_packages |   current_affected_products |
|:------------------------------------|----------------------------:|----------------------------:|
| Hospira, Inc., a Pfizer Company     |                         168 |                         102 |
| Fresenius Kabi USA, LLC             |                         157 |                          91 |
| Hikma Pharmaceuticals USA, Inc.     |                          92 |                          86 |
| Baxter Healthcare                   |                          62 |                          36 |
| Eugia US LLC                        |                          48 |                          48 |
| Teva Pharmaceuticals USA, Inc.      |                          44 |                          40 |
| Pfizer Inc.                         |                          36 |                          27 |
| Accord Healthcare Inc.              |                          26 |                          14 |
| Otsuka ICU Medical LLC              |                          24 |                          12 |
| Gland Pharma Limited                |                          24 |                          13 |
| Sun Pharmaceutical Industries, Inc. |                          22 |                          18 |
| Elite Laboratories, Inc.            |                          20 |                          17 |
| SpecGx LLC                          |                          20 |                          19 |
| B. Braun Medical Inc.               |                          19 |                          10 |
| Aurobindo Pharma USA                |                          18 |                          15 |

## Package types most affected (current)
| package_type   |   shortage_count |
|:---------------|-----------------:|
| Vial           |              531 |
| Bottle         |              339 |
| Other/Unknown  |              116 |
| Carton         |              114 |
| Blister Pack   |                6 |

---
# Full SQL outputs

## Results from `monitoring/schema_snapshot.sql`

### Statement 1
```sql
SELECT table_name
FROM information_schema.tables
WHERE table_schema = DATABASE()
  AND table_type = 'BASE TABLE'
ORDER BY table_name;
```
| TABLE_NAME         |
|:-------------------|
| raw_drug_shortages |
| raw_ndc            |
| raw_ndc_packaging  |
| shortage_contacts  |
| shortages_with_ndc |

### Statement 2
```sql
SELECT table_name, column_name, data_type, is_nullable
FROM information_schema.columns
WHERE table_schema = DATABASE()
ORDER BY table_name, ordinal_position;
```
| TABLE_NAME                 | COLUMN_NAME               | DATA_TYPE   | IS_NULLABLE   |
|:---------------------------|:--------------------------|:------------|:--------------|
| current_manufacturer_risk  | company_name              | varchar     | YES           |
| current_manufacturer_risk  | current_affected_packages | bigint      | NO            |
| current_manufacturer_risk  | current_affected_products | bigint      | NO            |
| current_package_shortages  | generic_name              | text        | YES           |
| current_package_shortages  | company_name              | varchar     | YES           |
| current_package_shortages  | status                    | varchar     | YES           |
| current_package_shortages  | product_ndc               | varchar     | YES           |
| current_package_shortages  | package_ndc               | varchar     | YES           |
| current_package_shortages  | package_description       | text        | YES           |
| current_package_shortages  | therapeutic_category      | text        | YES           |
| current_package_shortages  | initial_posting_date      | varchar     | YES           |
| current_package_shortages  | update_date               | varchar     | YES           |
| manufacturer_risk_analysis | company_name              | varchar     | YES           |
| manufacturer_risk_analysis | affected_packages         | bigint      | NO            |
| manufacturer_risk_analysis | affected_products         | bigint      | NO            |
| manufacturer_risk_analysis | current_shortage_packages | bigint      | NO            |
| multi_package_shortages    | product_ndc               | varchar     | YES           |
| multi_package_shortages    | generic_name              | text        | YES           |
| multi_package_shortages    | manufacturer              | varchar     | YES           |
| multi_package_shortages    | affected_packages         | bigint      | NO            |
| raw_drug_shortages         | shortage_id               | int         | NO            |
| raw_drug_shortages         | package_ndc               | varchar     | YES           |
| raw_drug_shortages         | generic_name              | text        | YES           |
| raw_drug_shortages         | company_name              | text        | YES           |
| raw_drug_shortages         | status                    | varchar     | YES           |

### Statement 3
```sql
SELECT table_name AS view_name
FROM information_schema.views
WHERE table_schema = DATABASE()
ORDER BY table_name;
```
| view_name                  |
|:---------------------------|
| current_manufacturer_risk  |
| current_package_shortages  |
| manufacturer_risk_analysis |
| multi_package_shortages    |

### Statement 4
```sql
SHOW CREATE VIEW current_package_shortages;
```
| View                      | Create View                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           | character_set_client   | collation_connection   |
|:--------------------------|:------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|:-----------------------|:-----------------------|
| current_package_shortages | CREATE ALGORITHM=UNDEFINED DEFINER=`root`@`localhost` SQL SECURITY DEFINER VIEW `current_package_shortages` AS select distinct `shortages_with_ndc`.`shortage_generic_name` AS `generic_name`,`shortages_with_ndc`.`company_name` AS `company_name`,`shortages_with_ndc`.`status` AS `status`,`shortages_with_ndc`.`product_ndc` AS `product_ndc`,`shortages_with_ndc`.`package_ndc` AS `package_ndc`,`shortages_with_ndc`.`package_description` AS `package_description`,`shortages_with_ndc`.`therapeutic_category` AS `therapeutic_category`,`shortages_with_ndc`.`initial_posting_date` AS `initial_posting_date`,`shortages_with_ndc`.`update_date` AS `update_date` from `shortages_with_ndc` where (`shortages_with_ndc`.`status` = 'Current') | cp850                  | cp850_general_ci       |

### Statement 5
```sql
SHOW CREATE VIEW multi_package_shortages;
```
| View                    | Create View                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          | character_set_client   | collation_connection   |
|:------------------------|:-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|:-----------------------|:-----------------------|
| multi_package_shortages | CREATE ALGORITHM=UNDEFINED DEFINER=`root`@`localhost` SQL SECURITY DEFINER VIEW `multi_package_shortages` AS select `shortages_with_ndc`.`product_ndc` AS `product_ndc`,`shortages_with_ndc`.`shortage_generic_name` AS `generic_name`,`shortages_with_ndc`.`company_name` AS `manufacturer`,count(distinct `shortages_with_ndc`.`package_ndc`) AS `affected_packages` from `shortages_with_ndc` where (`shortages_with_ndc`.`product_ndc` is not null) group by `shortages_with_ndc`.`product_ndc`,`shortages_with_ndc`.`shortage_generic_name`,`shortages_with_ndc`.`company_name` having (count(distinct `shortages_with_ndc`.`package_ndc`) > 1) | cp850                  | cp850_general_ci       |

### Statement 6
```sql
SHOW CREATE VIEW manufacturer_risk_analysis;
```
| View                       | Create View                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        | character_set_client   | collation_connection   |
|:---------------------------|:-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|:-----------------------|:-----------------------|
| manufacturer_risk_analysis | CREATE ALGORITHM=UNDEFINED DEFINER=`root`@`localhost` SQL SECURITY DEFINER VIEW `manufacturer_risk_analysis` AS select `shortages_with_ndc`.`company_name` AS `company_name`,count(distinct `shortages_with_ndc`.`package_ndc`) AS `affected_packages`,count(distinct `shortages_with_ndc`.`product_ndc`) AS `affected_products`,count(distinct (case when (`shortages_with_ndc`.`status` = 'Current') then `shortages_with_ndc`.`package_ndc` end)) AS `current_shortage_packages` from `shortages_with_ndc` where (`shortages_with_ndc`.`company_name` is not null) group by `shortages_with_ndc`.`company_name` | cp850                  | cp850_general_ci       |

### Statement 7
```sql
SHOW CREATE VIEW current_manufacturer_risk;
```
| View                      | Create View                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      | character_set_client   | collation_connection   |
|:--------------------------|:---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|:-----------------------|:-----------------------|
| current_manufacturer_risk | CREATE ALGORITHM=UNDEFINED DEFINER=`root`@`localhost` SQL SECURITY DEFINER VIEW `current_manufacturer_risk` AS select `shortages_with_ndc`.`company_name` AS `company_name`,count(distinct `shortages_with_ndc`.`package_ndc`) AS `current_affected_packages`,count(distinct `shortages_with_ndc`.`product_ndc`) AS `current_affected_products` from `shortages_with_ndc` where ((`shortages_with_ndc`.`status` = 'Current') and (`shortages_with_ndc`.`company_name` is not null)) group by `shortages_with_ndc`.`company_name` | cp850                  | cp850_general_ci       |

### Statement 8
```sql
SHOW CREATE TABLE raw_ndc;
```
| Table   | Create Table                                                       |
|:--------|:-------------------------------------------------------------------|
| raw_ndc | CREATE TABLE `raw_ndc` (                                           |
|         |   `product_ndc` varchar(20) NOT NULL,                              |
|         |   `generic_name` text,                                             |
|         |   `labeler_name` text,                                             |
|         |   `brand_name` text,                                               |
|         |   `finished` tinyint(1) DEFAULT NULL,                              |
|         |   `marketing_category` varchar(100) DEFAULT NULL,                  |
|         |   `dosage_form` text,                                              |
|         |   `route` text,                                                    |
|         |   `product_type` varchar(150) DEFAULT NULL,                        |
|         |   `marketing_start_date` varchar(20) DEFAULT NULL,                 |
|         |   `application_number` varchar(50) DEFAULT NULL,                   |
|         |   PRIMARY KEY (`product_ndc`),                                     |
|         |   KEY `idx_labeler` (`labeler_name`(255)),                         |
|         |   KEY `idx_brand` (`brand_name`(255))                              |
|         | ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci |

### Statement 9
```sql
SHOW CREATE TABLE raw_ndc_packaging;
```
| Table             | Create Table                                                                                                                                 |
|:------------------|:---------------------------------------------------------------------------------------------------------------------------------------------|
| raw_ndc_packaging | CREATE TABLE `raw_ndc_packaging` (                                                                                                           |
|                   |   `package_ndc` varchar(30) NOT NULL,                                                                                                        |
|                   |   `product_ndc` varchar(20) DEFAULT NULL,                                                                                                    |
|                   |   `description` text,                                                                                                                        |
|                   |   `marketing_start_date` varchar(20) DEFAULT NULL,                                                                                           |
|                   |   PRIMARY KEY (`package_ndc`),                                                                                                               |
|                   |   KEY `idx_product_ndc` (`product_ndc`),                                                                                                     |
|                   |   CONSTRAINT `raw_ndc_packaging_ibfk_1` FOREIGN KEY (`product_ndc`) REFERENCES `raw_ndc` (`product_ndc`) ON DELETE CASCADE ON UPDATE CASCADE |
|                   | ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci                                                                           |

### Statement 10
```sql
SHOW CREATE TABLE raw_drug_shortages;
```
| Table              | Create Table                                                                           |
|:-------------------|:---------------------------------------------------------------------------------------|
| raw_drug_shortages | CREATE TABLE `raw_drug_shortages` (                                                    |
|                    |   `shortage_id` int NOT NULL AUTO_INCREMENT,                                           |
|                    |   `package_ndc` varchar(30) DEFAULT NULL,                                              |
|                    |   `generic_name` text,                                                                 |
|                    |   `company_name` text,                                                                 |
|                    |   `status` varchar(50) DEFAULT NULL,                                                   |
|                    |   `therapeutic_category` text,                                                         |
|                    |   `initial_posting_date` varchar(20) DEFAULT NULL,                                     |
|                    |   `update_date` varchar(20) DEFAULT NULL,                                              |
|                    |   `dosage_form` text,                                                                  |
|                    |   `reason` text,                                                                       |
|                    |   PRIMARY KEY (`shortage_id`),                                                         |
|                    |   KEY `idx_package_ndc` (`package_ndc`),                                               |
|                    |   KEY `idx_status` (`status`),                                                         |
|                    |   KEY `idx_company` (`company_name`(255))                                              |
|                    | ) ENGINE=InnoDB AUTO_INCREMENT=1751 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci |

### Statement 11
```sql
SHOW CREATE TABLE shortages_with_ndc;
```
| Table              | Create Table                                                       |
|:-------------------|:-------------------------------------------------------------------|
| shortages_with_ndc | CREATE TABLE `shortages_with_ndc` (                                |
|                    |   `shortage_id` bigint DEFAULT NULL,                               |
|                    |   `package_ndc` varchar(30) DEFAULT NULL,                          |
|                    |   `shortage_generic_name` text,                                    |
|                    |   `company_name` varchar(255) DEFAULT NULL,                        |
|                    |   `status` varchar(50) DEFAULT NULL,                               |
|                    |   `therapeutic_category` text,                                     |
|                    |   `initial_posting_date` varchar(20) DEFAULT NULL,                 |
|                    |   `update_date` varchar(20) DEFAULT NULL,                          |
|                    |   `initial_posting_date_dt` date DEFAULT NULL,                     |
|                    |   `update_date_dt` date DEFAULT NULL,                              |
|                    |   `shortage_dosage_form` text,                                     |
|                    |   `reason` text,                                                   |
|                    |   `product_ndc` varchar(20) DEFAULT NULL,                          |
|                    |   `package_description` text,                                      |
|                    |   `package_marketing_start_date` varchar(20) DEFAULT NULL,         |
|                    |   `ndc_generic_name` text,                                         |
|                    |   `manufacturer` text,                                             |
|                    |   `brand_name` text,                                               |
|                    |   `finished` tinyint(1) DEFAULT NULL,                              |
|                    |   `marketing_category` varchar(100) DEFAULT NULL,                  |
|                    |   `ndc_dosage_form` text,                                          |
|                    |   `route` text,                                                    |
|                    |   `product_type` varchar(100) DEFAULT NULL,                        |
|                    |   `application_number` varchar(50) DEFAULT NULL,                   |
|                    |   KEY `idx_status` (`status`),                                     |
|                    |   KEY `idx_company` (`company_name`),                              |
|                    |   KEY `idx_product_ndc` (`product_ndc`),                           |
|                    |   KEY `idx_initial_posting_date_dt` (`initial_posting_date_dt`),   |
|                    |   KEY `idx_update_date_dt` (`update_date_dt`)                      |
|                    | ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci |

## Results from `monitoring/pipeline_health.sql`

### Statement 2
```sql
SELECT
    table_name,
    'exists' AS status
FROM information_schema.tables
WHERE table_schema = 'fda_shortage_db'
  AND table_name IN (
      'raw_ndc',
      'raw_ndc_packaging',
      'raw_drug_shortages',
      'shortage_contacts',
      'shortages_with_ndc'
  )
ORDER BY table_name;
```
| TABLE_NAME         | status   |
|:-------------------|:---------|
| raw_drug_shortages | exists   |
| raw_ndc            | exists   |
| raw_ndc_packaging  | exists   |
| shortage_contacts  | exists   |
| shortages_with_ndc | exists   |

### Statement 3
```sql
SELECT 'raw_ndc' AS table_name, COUNT(*) AS row_count FROM raw_ndc
UNION ALL
SELECT 'raw_ndc_packaging', COUNT(*) FROM raw_ndc_packaging
UNION ALL
SELECT 'raw_drug_shortages', COUNT(*) FROM raw_drug_shortages
UNION ALL
SELECT 'shortages_with_ndc', COUNT(*) FROM shortages_with_ndc;
```
| table_name         |   row_count |
|:-------------------|------------:|
| raw_ndc            |      128780 |
| raw_ndc_packaging  |      244838 |
| raw_drug_shortages |        1750 |
| shortages_with_ndc |        1750 |

### Statement 4
```sql
SELECT
    'shortages_with_ndc_status' AS check_name,
    CASE
        WHEN COUNT(*) > 0 THEN 'PASS'
        ELSE 'FAIL'
    END AS result,
    COUNT(*) AS row_count
FROM shortages_with_ndc;
```
| check_name                | result   |   row_count |
|:--------------------------|:---------|------------:|
| shortages_with_ndc_status | PASS     |        1750 |

### Statement 5
```sql
SELECT
    'latest_update_date' AS metric,
    MAX(update_date) AS most_recent_update
FROM shortages_with_ndc;
```
| metric             | most_recent_update   |
|:-------------------|:---------------------|
| latest_update_date | 12/29/2025           |

### Statement 6
```sql
SELECT
    table_name AS view_name,
    'available' AS status
FROM information_schema.views
WHERE table_schema = 'fda_shortage_db'
  AND table_name IN (
      'current_package_shortages',
      'multi_package_shortages',
      'manufacturer_risk_analysis',
      'current_manufacturer_risk'
  )
ORDER BY table_name;
```
| view_name                  | status    |
|:---------------------------|:----------|
| current_manufacturer_risk  | available |
| current_package_shortages  | available |
| manufacturer_risk_analysis | available |
| multi_package_shortages    | available |

## Results from `monitoring/data_quality_checks.sql`

### Statement 2
```sql
SELECT
    'ndc_join_coverage' AS metric,
    COUNT(*) AS total_rows,
    SUM(product_ndc IS NOT NULL) AS joined_rows,
    SUM(product_ndc IS NULL) AS unjoined_rows,
    ROUND(
        SUM(product_ndc IS NOT NULL) * 100.0 / NULLIF(COUNT(*), 0),
        2
    ) AS join_success_pct
FROM shortages_with_ndc;
```
| metric            |   total_rows |   joined_rows |   unjoined_rows |   join_success_pct |
|:------------------|-------------:|--------------:|----------------:|-------------------:|
| ndc_join_coverage |         1750 |          1578 |             172 |              90.17 |

### Statement 3
```sql
SELECT
    'missing_package_ndc' AS metric,
    COUNT(*) AS issue_count
FROM shortages_with_ndc
WHERE package_ndc IS NULL OR TRIM(package_ndc) = '';
```
| metric              |   issue_count |
|:--------------------|--------------:|
| missing_package_ndc |             0 |

### Statement 4
```sql
SELECT
    'missing_company_name' AS metric,
    COUNT(*) AS issue_count
FROM shortages_with_ndc
WHERE company_name IS NULL OR TRIM(company_name) = '';
```
| metric               |   issue_count |
|:---------------------|--------------:|
| missing_company_name |             0 |

### Statement 5
```sql
SELECT
    'missing_status' AS metric,
    COUNT(*) AS issue_count
FROM shortages_with_ndc
WHERE status IS NULL OR TRIM(status) = '';
```
| metric         |   issue_count |
|:---------------|--------------:|
| missing_status |             0 |

### Statement 6
```sql
SELECT
    'duplicate_shortage_ids' AS metric,
    COUNT(*) AS duplicate_count
FROM (
    SELECT shortage_id
    FROM shortages_with_ndc
    GROUP BY shortage_id
    HAVING COUNT(*) > 1
) duplicates;
```
| metric                 |   duplicate_count |
|:-----------------------|------------------:|
| duplicate_shortage_ids |                 0 |

### Statement 7
```sql
SELECT
    'invalid_initial_posting_date_dt' AS metric,
    COUNT(*) AS invalid_count
FROM shortages_with_ndc
WHERE initial_posting_date IS NOT NULL
  AND initial_posting_date <> ''
  AND initial_posting_date NOT REGEXP '^[0-9]{8}$'
  AND initial_posting_date NOT REGEXP '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}$';
```
| metric                          |   invalid_count |
|:--------------------------------|----------------:|
| invalid_initial_posting_date_dt |               0 |

### Statement 8
```sql
SELECT
    'invalid_update_date_dt' AS metric,
    COUNT(*) AS invalid_count
FROM shortages_with_ndc
WHERE update_date IS NOT NULL
  AND update_date <> ''
  AND update_date NOT REGEXP '^[0-9]{8}$'
  AND update_date NOT REGEXP '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}$';
```
| metric                 |   invalid_count |
|:-----------------------|----------------:|
| invalid_update_date_dt |               0 |

### Statement 9
```sql
SELECT
    'status_summary' AS metric,
    status,
    COUNT(*) AS row_count
FROM shortages_with_ndc
GROUP BY status
ORDER BY row_count DESC;
```
| metric         | status             |   row_count |
|:---------------|:-------------------|------------:|
| status_summary | Current            |        1175 |
| status_summary | To Be Discontinued |         519 |
| status_summary | Resolved           |          56 |

### Statement 10
```sql
SELECT
    shortage_id,
    package_ndc,
    shortage_generic_name,
    company_name,
    status
FROM shortages_with_ndc
WHERE product_ndc IS NULL
ORDER BY shortage_id DESC
LIMIT 15;
```
|   shortage_id | package_ndc   | shortage_generic_name                    | company_name                            | status             |
|--------------:|:--------------|:-----------------------------------------|:----------------------------------------|:-------------------|
|          1745 | 83090-007-10  | Lidocaine Hydrochloride Injection        | Sintetica US                            | Current            |
|          1744 | 83090-006-10  | Lidocaine Hydrochloride Injection        | Sintetica US                            | Current            |
|          1743 | 83090-005-10  | Lidocaine Hydrochloride Injection        | Sintetica US                            | Current            |
|          1742 | 83090-004-10  | Lidocaine Hydrochloride Injection        | Sintetica US                            | Current            |
|          1741 | 83090-003-10  | Lidocaine Hydrochloride Injection        | Sintetica US                            | Current            |
|          1740 | 83090-002-10  | Lidocaine Hydrochloride Injection        | Sintetica US                            | Current            |
|          1739 | 83090-001-10  | Lidocaine Hydrochloride Injection        | Sintetica US                            | Current            |
|          1737 | 82497-025-04  | Methotrexate Injection                   | Assertio Specialty Pharmaceuticals, LLC | To Be Discontinued |
|          1736 | 82497-022-04  | Methotrexate Injection                   | Assertio Specialty Pharmaceuticals, LLC | To Be Discontinued |
|          1735 | 82497-020-04  | Methotrexate Injection                   | Assertio Specialty Pharmaceuticals, LLC | To Be Discontinued |
|          1734 | 82497-017-04  | Methotrexate Injection                   | Assertio Specialty Pharmaceuticals, LLC | To Be Discontinued |
|          1733 | 82497-015-04  | Methotrexate Injection                   | Assertio Specialty Pharmaceuticals, LLC | To Be Discontinued |
|          1732 | 82497-012-04  | Methotrexate Injection                   | Assertio Specialty Pharmaceuticals, LLC | To Be Discontinued |
|          1731 | 82497-010-04  | Methotrexate Injection                   | Assertio Specialty Pharmaceuticals, LLC | To Be Discontinued |
|          1712 | 76045-106-10  | Dexamethasone Sodium Phosphate Injection | Fresenius Kabi USA, LLC                 | Current            |

## Results from `sql/03_analysis_queries.sql`

### Statement 2
```sql
SELECT 
    company_name,
    current_affected_packages,
    current_affected_products
FROM current_manufacturer_risk
LIMIT 10;
```
| company_name                    |   current_affected_packages |   current_affected_products |
|:--------------------------------|----------------------------:|----------------------------:|
| Accord Healthcare Inc.          |                          26 |                          14 |
| Ailex Pharmaceuticals           |                           1 |                           1 |
| Alembic Pharmaceuticals         |                           3 |                           3 |
| Alvogen                         |                          12 |                          12 |
| American Regent, Inc.           |                           2 |                           2 |
| Amneal Pharmaceuticals          |                          14 |                          10 |
| Amphastar Pharmaceuticals, Inc. |                           6 |                           6 |
| Apotex Corp.                    |                           8 |                           8 |
| Armas Pharmaceuticals Inc       |                           1 |                           1 |
| Aurobindo Pharma USA            |                          18 |                          15 |

### Statement 3
```sql
SELECT 
    CASE 
        WHEN brand_name IS NOT NULL AND brand_name != '' THEN 'Branded Drug'
        ELSE 'Generic/Unbranded'
    END AS drug_type,
    COUNT(*) AS shortage_count,
    COUNT(DISTINCT company_name) AS manufacturers_affected,
    ROUND(AVG(DATEDIFF(CURDATE(),initial_posting_date_dt)), 0) AS avg_days_in_shortage
FROM shortages_with_ndc
WHERE status = 'Current'
    AND initial_posting_date_dt IS NOT NULL
GROUP BY drug_type
ORDER BY shortage_count DESC;
```
| drug_type         |   shortage_count |   manufacturers_affected |   avg_days_in_shortage |
|:------------------|-----------------:|-------------------------:|-----------------------:|
| Branded Drug      |             1097 |                       90 |                   1982 |
| Generic/Unbranded |               78 |                       22 |                   2207 |

### Statement 4
```sql
SELECT 
    generic_name,
    manufacturer,
    affected_packages,
    product_ndc
FROM multi_package_shortages
LIMIT 15;
```
| generic_name                              | manufacturer                    |   affected_packages | product_ndc   |
|:------------------------------------------|:--------------------------------|--------------------:|:--------------|
| Lasmiditan Succinate Tablet               | Eli Lilly and Co.               |                   3 | 0002-4312     |
| Lasmiditan Succinate Tablet               | Eli Lilly and Co.               |                   3 | 0002-4491     |
| Insulin Lispro-aabc Injection             | Eli Lilly and Co.               |                   2 | 0002-8235     |
| Hydrocortisone Sodium Succinate Injection | Hospira, Inc., a Pfizer Company |                   2 | 0009-0013     |
| Methylprednisolone Acetate Injection      | Pfizer Inc.                     |                   4 | 0009-0280     |
| Methylprednisolone Acetate Injection      | Pfizer Inc.                     |                   2 | 0009-0306     |
| Methylprednisolone Acetate Injection      | Pfizer Inc.                     |                   4 | 0009-3073     |
| Methylprednisolone Acetate Injection      | Pfizer Inc.                     |                   2 | 0009-3475     |
| Carbamazepine Tablet                      | Teva Pharmaceuticals USA, Inc.  |                   2 | 0093-0109     |
| Fluocinonide Cream and Ointment           | Teva Pharmaceuticals USA, Inc.  |                   3 | 0093-0262     |
| Fluocinonide Cream and Ointment           | Teva Pharmaceuticals USA, Inc.  |                   3 | 0093-0263     |
| Fluocinonide Cream and Ointment           | Teva Pharmaceuticals USA, Inc.  |                   3 | 0093-0264     |
| Pravastatin Sodium Tablet                 | Teva Pharmaceuticals USA, Inc.  |                   2 | 0093-0771     |
| Clonazepam Tablet                         | Teva Pharmaceuticals USA, Inc.  |                   2 | 0093-0832     |
| Clonazepam Tablet                         | Teva Pharmaceuticals USA, Inc.  |                   2 | 0093-3212     |

### Statement 5
```sql
SELECT 
    product_type,
    COUNT(*) AS current_shortages,
    COUNT(DISTINCT company_name) AS manufacturers,
    ROUND(AVG(DATEDIFF(CURDATE(),initial_posting_date_dt)), 0) AS avg_days_active,
    MAX(DATEDIFF(CURDATE(),initial_posting_date_dt)) AS longest_active_days
FROM shortages_with_ndc
WHERE status = 'Current'
    AND product_type IS NOT NULL
    AND initial_posting_date_dt IS NOT NULL
GROUP BY product_type
ORDER BY current_shortages DESC;
```
| product_type                |   current_shortages |   manufacturers |   avg_days_active |   longest_active_days |
|:----------------------------|--------------------:|----------------:|------------------:|----------------------:|
| HUMAN PRESCRIPTION DRUG     |                1097 |              90 |              1982 |                  5152 |
| DRUG FOR FURTHER PROCESSING |                   9 |               1 |              1105 |                  1116 |

### Statement 6
```sql
SELECT 
    CASE 
        WHEN package_description LIKE '%bottle%' THEN 'Bottle'
        WHEN package_description LIKE '%vial%' THEN 'Vial'
        WHEN package_description LIKE '%blister%' THEN 'Blister Pack'
        WHEN package_description LIKE '%carton%' THEN 'Carton'
        WHEN package_description LIKE '%kit%' THEN 'Kit'
        ELSE 'Other/Unknown'
    END AS package_type,
    COUNT(*) AS shortage_count,
    COUNT(DISTINCT company_name) AS manufacturers
FROM shortages_with_ndc
WHERE status = 'Current'
    AND package_description IS NOT NULL
GROUP BY package_type
ORDER BY shortage_count DESC;
```
| package_type   |   shortage_count |   manufacturers |
|:---------------|-----------------:|----------------:|
| Vial           |              531 |              47 |
| Bottle         |              339 |              40 |
| Other/Unknown  |              116 |              12 |
| Carton         |              114 |              26 |
| Blister Pack   |                6 |               2 |

### Statement 7
```sql
SELECT 
    CASE 
        WHEN route LIKE '%ORAL%' THEN 'Oral'
        WHEN route LIKE '%INTRAVENOUS%' OR route LIKE '%IV%' THEN 'Intravenous'
        WHEN route LIKE '%INJECTION%' THEN 'Injection'
        WHEN route LIKE '%TOPICAL%' THEN 'Topical'
        WHEN route LIKE '%INHALATION%' THEN 'Inhalation'
        ELSE 'Other'
    END AS administration_route,
    COUNT(*) AS shortage_count,
    COUNT(DISTINCT product_ndc) AS products_affected
FROM shortages_with_ndc
WHERE status = 'Current'
    AND route IS NOT NULL
GROUP BY administration_route
ORDER BY shortage_count DESC;
```
| administration_route   |   shortage_count |   products_affected |
|:-----------------------|-----------------:|--------------------:|
| Intravenous            |              540 |                 416 |
| Oral                   |              304 |                 263 |
| Other                  |              249 |                 163 |
| Inhalation             |                3 |                   3 |
| Topical                |                1 |                   1 |

### Statement 8
```sql
SELECT 
    'Total Shortage Records' AS metric,
    COUNT(*) AS count,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM shortages_with_ndc), 2) AS percentage
FROM shortages_with_ndc
UNION ALL
SELECT 
    'Matched with NDC Data' AS metric,
    COUNT(*) AS count,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM shortages_with_ndc), 2) AS percentage
FROM shortages_with_ndc
WHERE product_ndc IS NOT NULL
UNION ALL
SELECT 
    'Unmatched (No NDC Found)' AS metric,
    COUNT(*) AS count,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM shortages_with_ndc), 2) AS percentage
FROM shortages_with_ndc
WHERE product_ndc IS NULL;
```
| metric                   |   count |   percentage |
|:-------------------------|--------:|-------------:|
| Total Shortage Records   |    1750 |       100    |
| Matched with NDC Data    |    1578 |        90.17 |
| Unmatched (No NDC Found) |     172 |         9.83 |

### Statement 9
```sql
SELECT 
    marketing_category,
    COUNT(*) AS shortage_count,
    COUNT(DISTINCT company_name) AS manufacturers,
    COUNT(DISTINCT product_ndc) AS products
FROM shortages_with_ndc
WHERE status = 'Current'
    AND marketing_category IS NOT NULL
GROUP BY marketing_category
ORDER BY shortage_count DESC;
```
| marketing_category                       |   shortage_count |   manufacturers |   products |
|:-----------------------------------------|-----------------:|----------------:|-----------:|
| ANDA                                     |              734 |              67 |        601 |
| NDA                                      |              344 |              24 |        227 |
| DRUG FOR FURTHER PROCESSING              |                9 |               1 |          7 |
| NDA AUTHORIZED GENERIC                   |                8 |               3 |          7 |
| UNAPPROVED DRUG FOR USE IN DRUG SHORTAGE |                8 |               5 |          8 |
| BLA                                      |                2 |               1 |          2 |
| UNAPPROVED DRUG OTHER                    |                1 |               1 |          1 |

### Statement 10
```sql
SELECT 
    company_name AS manufacturer,
    shortage_generic_name AS generic_name,
    brand_name,
    manufacturer AS ndc_labeler,
    shortage_dosage_form AS dosage_form,
    route AS administration_route,
    package_description,
    product_type,
    initial_posting_date AS posted_date,
    DATEDIFF(CURDATE(),initial_posting_date_dt) AS days_active
FROM shortages_with_ndc
WHERE status = 'Current'
    AND product_ndc IS NOT NULL
    AND initial_posting_date_dt IS NOT NULL
ORDER BY days_active DESC
LIMIT 50;
```
| manufacturer                    | generic_name                                              | brand_name                              | ndc_labeler             | dosage_form                                                                                                                       | administration_route                                                               | package_description                                                                                                   | product_type            | posted_date   |   days_active |
|:--------------------------------|:----------------------------------------------------------|:----------------------------------------|:------------------------|:----------------------------------------------------------------------------------------------------------------------------------|:-----------------------------------------------------------------------------------|:----------------------------------------------------------------------------------------------------------------------|:------------------------|:--------------|--------------:|
| Fresenius Kabi USA, LLC         | Fentanyl Citrate Injection                                | Fentanyl Citrate                        | Fresenius Kabi USA, LLC | Fentanyl Citrate Preservative Free, Injection, .05 mg/1 mL (NDC 63323-806-02)                                                     | ['INTRAMUSCULAR', 'INTRAVENOUS']                                                   | 25 VIAL, SINGLE-DOSE in 1 CARTON (63323-806-02)  / 2 mL in 1 VIAL, SINGLE-DOSE (63323-806-12)                         | HUMAN PRESCRIPTION DRUG | 01/01/2012    |          5152 |
| Fresenius Kabi USA, LLC         | Atropine Sulfate Injection                                | Atropine Sulfate                        | Fresenius Kabi USA, LLC | Atropine Sulfate, Injection, .4 mg/1 mL (NDC 63323-580-20)                                                                        | ['ENDOTRACHEAL', 'INTRAMEDULLARY', 'INTRAMUSCULAR', 'INTRAVENOUS', 'SUBCUTANEOUS'] | 10 VIAL, MULTI-DOSE in 1 TRAY (63323-580-20)  / 20 mL in 1 VIAL, MULTI-DOSE                                           | HUMAN PRESCRIPTION DRUG | 01/01/2012    |          5152 |
| Fresenius Kabi USA, LLC         | Fentanyl Citrate Injection                                | Fentanyl Citrate                        | Fresenius Kabi USA, LLC | Fentanyl Citrate Preservative Free, Injection, .05 mg/1 mL (NDC 63323-806-01)                                                     | ['INTRAMUSCULAR', 'INTRAVENOUS']                                                   | 25 VIAL, SINGLE-DOSE in 1 CARTON (63323-806-01)  / 1 mL in 1 VIAL, SINGLE-DOSE (63323-806-11)                         | HUMAN PRESCRIPTION DRUG | 01/01/2012    |          5152 |
| Fresenius Kabi USA, LLC         | Fentanyl Citrate Injection                                | Fentanyl Citrate                        | Fresenius Kabi USA, LLC | Fentanyl Citrate Preservative Free, Injection, .05 mg/1 mL (NDC 63323-806-05)                                                     | ['INTRAMUSCULAR', 'INTRAVENOUS']                                                   | 25 VIAL, SINGLE-DOSE in 1 CARTON (63323-806-05)  / 5 mL in 1 VIAL, SINGLE-DOSE (63323-806-13)                         | HUMAN PRESCRIPTION DRUG | 01/01/2012    |          5152 |
| Fresenius Kabi USA, LLC         | Fentanyl Citrate Injection                                | Fentanyl Citrate                        | Fresenius Kabi USA, LLC | Fentanyl Citrate Preservative Free, Injection, .05 mg/1 mL (NDC 63323-806-20)                                                     | ['INTRAMUSCULAR', 'INTRAVENOUS']                                                   | 25 VIAL, SINGLE-DOSE in 1 CARTON (63323-806-20)  / 20 mL in 1 VIAL, SINGLE-DOSE (63323-806-14)                        | HUMAN PRESCRIPTION DRUG | 01/01/2012    |          5152 |
| Fresenius Kabi USA, LLC         | Fentanyl Citrate Injection                                | Fentanyl Citrate                        | Fresenius Kabi USA, LLC | Fentanyl Citrate Preservative Free, Injection, .05 mg/1 mL (NDC 63323-806-50)                                                     | ['INTRAMUSCULAR', 'INTRAVENOUS']                                                   | 1 VIAL, SINGLE-DOSE in 1 CARTON (63323-806-50)  / 50 mL in 1 VIAL, SINGLE-DOSE                                        | HUMAN PRESCRIPTION DRUG | 01/01/2012    |          5152 |
| Fresenius Kabi USA, LLC         | Fentanyl Citrate Injection                                | Fentanyl Citrate                        | Fresenius Kabi USA, LLC | Fentanyl Citrate, Injection, 50 ug/1 mL (NDC 63323-808-11)                                                                        | ['INTRAMUSCULAR', 'INTRAVENOUS']                                                   | 10 SYRINGE in 1 CARTON (63323-808-11)  / 1 mL in 1 SYRINGE (63323-808-01)                                             | HUMAN PRESCRIPTION DRUG | 01/01/2012    |          5152 |
| Medefil, Inc.                   | Atropine Sulfate Injection                                | Atropine Sulfate                        | Medefil, Inc.           | Atropine Sulfate, Injection, .1 mg/1 mL (NDC 64253-400-91)                                                                        | ['INTRAVENOUS']                                                                    | 10 SYRINGE, PLASTIC in 1 BOX (64253-400-91)  / 10 mL in 1 SYRINGE, PLASTIC (64253-400-30)                             | HUMAN PRESCRIPTION DRUG | 01/01/2012    |          5152 |
| Hospira, Inc., a Pfizer Company | Atropine Sulfate Injection                                | Atropine Sulfate                        | Hospira, Inc.           | Atropine Sulfate, Injection, 0.25 mg/5 mL (0.05 mg/mL) Syringes (NDC 0409-9630-05)                                                | ['INTRAVENOUS']                                                                    | 10 CARTON in 1 PACKAGE (0409-9630-05)  / 1 SYRINGE, PLASTIC in 1 CARTON / 5 mL in 1 SYRINGE, PLASTIC (0409-9630-15)   | HUMAN PRESCRIPTION DRUG | 01/01/2012    |          5152 |
| Hospira, Inc., a Pfizer Company | Fentanyl Citrate Injection                                | Fentanyl Citrate                        | Hospira, Inc.           | Fentanyl Citrate, Injection, 2500 mcg/50 mL (50 ug/1 mL) (NDC 0409-9094-61)                                                       | ['INTRAMUSCULAR', 'INTRAVENOUS']                                                   | 25 CARTON in 1 TRAY (0409-9094-61)  / 1 VIAL, SINGLE-DOSE in 1 CARTON / 50 mL in 1 VIAL, SINGLE-DOSE (0409-9094-41)   | HUMAN PRESCRIPTION DRUG | 01/01/2012    |          5152 |
| Hospira, Inc., a Pfizer Company | Fentanyl Citrate Injection                                | Fentanyl Citrate                        | Hospira, Inc.           | Fentanyl Citrate, Injection, 500 mcg/10 mL (50 ug/1 mL) (NDC 0409-9094-28)                                                        | ['INTRAMUSCULAR', 'INTRAVENOUS']                                                   | 25 VIAL, SINGLE-DOSE in 1 TRAY (0409-9094-28)  / 10 mL in 1 VIAL, SINGLE-DOSE (0409-9094-17)                          | HUMAN PRESCRIPTION DRUG | 01/01/2012    |          5152 |
| Accord Healthcare Inc.          | Atropine Sulfate Injection                                | Atropine Sulfate                        | Accord Healthcare Inc.  | Atropine Sulfate, Injection, .4 mg/1 mL (NDC 16729-512-43)                                                                        | ['ENDOTRACHEAL', 'INTRAMEDULLARY', 'INTRAMUSCULAR', 'INTRAVENOUS', 'SUBCUTANEOUS'] | 10 CARTON in 1 BOX (16729-512-43)  / 1 VIAL, MULTI-DOSE in 1 CARTON (16729-512-05)  / 20 mL in 1 VIAL, MULTI-DOSE     | HUMAN PRESCRIPTION DRUG | 01/01/2012    |          5152 |
| Hospira, Inc., a Pfizer Company | Atropine Sulfate Injection                                | Atropine Sulfate                        | Hospira, Inc.           | Atropine Sulfate, Injection, 1 mg/10 mL (0.1 mg/mL) Syringes (NDC 0409-1630-10)                                                   | ['INTRAVENOUS']                                                                    | 10 CARTON in 1 PACKAGE (0409-1630-10)  / 1 SYRINGE, PLASTIC in 1 CARTON / 10 mL in 1 SYRINGE, PLASTIC (0409-1630-15)  | HUMAN PRESCRIPTION DRUG | 01/01/2012    |          5152 |
| Hospira, Inc., a Pfizer Company | Fentanyl Citrate Injection                                | Fentanyl Citrate                        | Hospira, Inc.           | Fentanyl Citrate, Injection, 250 mcg/5 mL (50 ug/1 mL) (NDC 0409-9094-25)                                                         | ['INTRAMUSCULAR', 'INTRAVENOUS']                                                   | 25 VIAL, SINGLE-DOSE in 1 TRAY (0409-9094-25)  / 5 mL in 1 VIAL, SINGLE-DOSE (0409-9094-18)                           | HUMAN PRESCRIPTION DRUG | 01/01/2012    |          5152 |
| Hospira, Inc., a Pfizer Company | Fentanyl Citrate Injection                                | Fentanyl Citrate                        | Hospira, Inc.           | Fentanyl Citrate, Injection, 1000 mcg/20 mL (50 ug/1 mL) (NDC 0409-9094-31)                                                       | ['INTRAMUSCULAR', 'INTRAVENOUS']                                                   | 25 VIAL, SINGLE-DOSE in 1 TRAY (0409-9094-31)  / 20 mL in 1 VIAL, SINGLE-DOSE (0409-9094-16)                          | HUMAN PRESCRIPTION DRUG | 01/01/2012    |          5152 |
| American Regent, Inc.           | Atropine Sulfate Injection                                | Atropine Sulfate                        | American Regent, Inc.   | Atropine Sulfate, Injection, .4 mg/1 mL (NDC 0517-1004-25)                                                                        | ['INTRAVENOUS']                                                                    | 25 VIAL, GLASS in 1 TRAY (0517-1004-25)  / 1 mL in 1 VIAL, GLASS (0517-1004-01)                                       | HUMAN PRESCRIPTION DRUG | 01/01/2012    |          5152 |
| American Regent, Inc.           | Atropine Sulfate Injection                                | Atropine Sulfate                        | American Regent, Inc.   | Atropine Sulfate, Injection, 1 mg/1 mL (NDC 0517-1001-25)                                                                         | ['INTRAVENOUS']                                                                    | 25 VIAL, GLASS in 1 TRAY (0517-1001-25)  / 1 mL in 1 VIAL, GLASS (0517-1001-01)                                       | HUMAN PRESCRIPTION DRUG | 01/01/2012    |          5152 |
| Hospira, Inc., a Pfizer Company | Fentanyl Citrate Injection                                | Fentanyl Citrate                        | Hospira, Inc.           | Fentanyl Citrate, Injection, 100 mcg/2 mL (50 ug/1 mL) (NDC 0409-9094-22)                                                         | ['INTRAMUSCULAR', 'INTRAVENOUS']                                                   | 25 VIAL, SINGLE-DOSE in 1 TRAY (0409-9094-22)  / 2 mL in 1 VIAL, SINGLE-DOSE (0409-9094-12)                           | HUMAN PRESCRIPTION DRUG | 01/01/2012    |          5152 |
| Hospira, Inc., a Pfizer Company | Lidocaine Hydrochloride Injection                         | Lidocaine Hydrochloride                 | Hospira, Inc.           | Lidocaine Hydrochloride Preservative Free In Plastic Container, Injection, 100 mg/5mL (2%,20 mg/1 mL) Syringes (NDC 0409-1323-05) | ['INTRAVENOUS']                                                                    | 10 CARTON in 1 CONTAINER (0409-1323-05)  / 1 SYRINGE, PLASTIC in 1 CARTON / 5 mL in 1 SYRINGE, PLASTIC (0409-1323-15) | HUMAN PRESCRIPTION DRUG | 02/22/2012    |          5100 |
| Hospira, Inc., a Pfizer Company | Lidocaine Hydrochloride Injection                         | Lidocaine Hydrochloride                 | Hospira, Inc.           | Lidocaine Hydrochloride, Injection, 100 mg/5 mL (2%; 20 mg/mL) (NDC 0409-2066-05)                                                 | ['EPIDURAL', 'INFILTRATION', 'INTRACAUDAL', 'PERINEURAL']                          | 10 VIAL, SINGLE-DOSE in 1 CARTON (0409-2066-05)  / 5 mL in 1 VIAL, SINGLE-DOSE (0409-2066-10)                         | HUMAN PRESCRIPTION DRUG | 02/22/2012    |          5100 |
| Hospira, Inc., a Pfizer Company | Lidocaine Hydrochloride Injection                         | Lidocaine Hydrochloride                 | Hospira, Inc.           | Lidocaine Hydrochloride, Injection, 200 mg/10 mL (2%; 20 mg/mL) (NDC 0409-4282-02)                                                | ['EPIDURAL', 'INFILTRATION', 'INTRACAUDAL', 'PERINEURAL']                          | 25 AMPULE in 1 CARTON (0409-4282-02)  / 10 mL in 1 AMPULE (0409-4282-12)                                              | HUMAN PRESCRIPTION DRUG | 02/22/2012    |          5100 |
| Hospira, Inc., a Pfizer Company | Epinephrine Bitartrate, Lidocaine Hydrochloride Injection | Lidocaine Hydrochloride and Epinephrine | Hospira, Inc.           | Lidocaine Hydrochloride And Epinephrine, Injection, 75 mg/5 mL (1.5%; 1:200,000) (NDC 0409-1209-01)                               | ['EPIDURAL']                                                                       | 10 AMPULE in 1 TRAY (0409-1209-01)  / 5 mL in 1 AMPULE (0409-1209-10)                                                 | HUMAN PRESCRIPTION DRUG | 02/22/2012    |          5100 |
| Hospira, Inc., a Pfizer Company | Epinephrine Bitartrate, Lidocaine Hydrochloride Injection | Lidocaine Hydrochloride and Epinephrine | Hospira, Inc.           | Lidocaine Hydrochloride And Epinephrine, Injection, 2.5 g/50 mL (0.5%; 1:200,000) (NDC 0409-3177-01)                              | ['INFILTRATION', 'PERINEURAL']                                                     | 25 VIAL, MULTI-DOSE in 1 TRAY (0409-3177-01)  / 50 mL in 1 VIAL, MULTI-DOSE (0409-3177-16)                            | HUMAN PRESCRIPTION DRUG | 02/22/2012    |          5100 |
| Hospira, Inc., a Pfizer Company | Lidocaine Hydrochloride Injection                         | Lidocaine Hydrochloride                 | Hospira, Inc.           | Lidocaine Hydrochloride Preservative Free In Plastic Container, Injection, 50 mg/5mL (1%, 10 mg/1 mL) Syringes (NDC 0409-9137-05) | ['INTRAVENOUS']                                                                    | 10 CARTON in 1 CONTAINER (0409-9137-05)  / 1 SYRINGE, PLASTIC in 1 CARTON / 5 mL in 1 SYRINGE, PLASTIC (0409-9137-11) | HUMAN PRESCRIPTION DRUG | 02/22/2012    |          5100 |
| Hospira, Inc., a Pfizer Company | Lidocaine Hydrochloride Injection                         | Lidocaine Hydrochloride                 | Hospira, Inc.           | Lidocaine Hydrochloride In Plastic Container, Injection, 250 mg/50 mL (0.5%; 5 mg/mL) (NDC 0409-4275-01)                          | ['INFILTRATION', 'PERINEURAL']                                                     | 25 VIAL, MULTI-DOSE in 1 TRAY (0409-4275-01)  / 50 mL in 1 VIAL, MULTI-DOSE (0409-4275-16)                            | HUMAN PRESCRIPTION DRUG | 02/22/2012    |          5100 |

### Statement 11
```sql
SELECT 
    s.company_name,
    COUNT(DISTINCT s.product_ndc) AS products_with_shortages,
    COUNT(DISTINCT n.product_ndc) AS total_ndc_portfolio,
    ROUND(COUNT(DISTINCT s.product_ndc) * 100.0 / COUNT(DISTINCT n.product_ndc), 2) AS shortage_rate_percent
FROM shortages_with_ndc s
LEFT JOIN raw_ndc n ON s.manufacturer = n.labeler_name
WHERE s.status = 'Current'
    AND s.company_name IS NOT NULL
GROUP BY s.company_name
HAVING COUNT(DISTINCT n.product_ndc) >= 5  -- Only manufacturers with 5+ products
ORDER BY products_with_shortages DESC
LIMIT 20;
```
| company_name                           |   products_with_shortages |   total_ndc_portfolio |   shortage_rate_percent |
|:---------------------------------------|--------------------------:|----------------------:|------------------------:|
| Hospira, Inc., a Pfizer Company        |                       102 |                   432 |                   23.61 |
| Fresenius Kabi USA, LLC                |                        91 |                   560 |                   16.25 |
| Hikma Pharmaceuticals USA, Inc.        |                        86 |                   789 |                   10.9  |
| Eugia US LLC                           |                        48 |                   238 |                   20.17 |
| Teva Pharmaceuticals USA, Inc.         |                        40 |                  1347 |                    2.97 |
| Baxter Healthcare                      |                        36 |                   380 |                    9.47 |
| Pfizer Inc.                            |                        27 |                   688 |                    3.92 |
| SpecGx LLC                             |                        19 |                   232 |                    8.19 |
| Sun Pharmaceutical Industries, Inc.    |                        18 |                   801 |                    2.25 |
| Elite Laboratories, Inc.               |                        17 |                    75 |                   22.67 |
| Lannett Company, Inc.                  |                        16 |                   185 |                    8.65 |
| Aurobindo Pharma USA                   |                        15 |                  1427 |                    1.05 |
| Accord Healthcare Inc.                 |                        14 |                   216 |                    6.48 |
| Mylan Institutional, a Viatris Company |                        14 |                   169 |                    8.28 |
| Takeda Pharmaceuticals USA Inc.        |                        13 |                   152 |                    8.55 |
| Gland Pharma Limited                   |                        13 |                  1105 |                    1.18 |
| Alvogen                                |                        12 |                    54 |                   22.22 |
| Otsuka ICU Medical LLC                 |                        12 |                    60 |                   20    |
| Solco Healthcare US, LLC               |                        11 |                   143 |                    7.69 |
| Amneal Pharmaceuticals                 |                        10 |                   442 |                    2.26 |

### Statement 12
```sql
SELECT 'Analysis queries ready - all showcase join value' AS status;
```
| status                                           |
|:-------------------------------------------------|
| Analysis queries ready - all showcase join value |
