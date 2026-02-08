--the main focus is to
--check join success,how many shortages matched to NDC
--check missing keys (package_ndc,company name,status)



USE fda_shortage_db;


--Join coverage check confirms how many shortage records are successfully linked to NDC product data.

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

--Checks missing identifiers that reduce analytical usefulness.

SELECT
    'missing_package_ndc' AS metric,
    COUNT(*) AS issue_count
FROM shortages_with_ndc
WHERE package_ndc IS NULL OR TRIM(package_ndc) = '';

SELECT
    'missing_company_name' AS metric,
    COUNT(*) AS issue_count
FROM shortages_with_ndc
WHERE company_name IS NULL OR TRIM(company_name) = '';

SELECT
    'missing_status' AS metric,
    COUNT(*) AS issue_count
FROM shortages_with_ndc
WHERE status IS NULL OR TRIM(status) = '';

--Duplicate primary identifier check ensures shortage_id behaves as a unique key.

SELECT
    'duplicate_shortage_ids' AS metric,
    COUNT(*) AS duplicate_count
FROM (
    SELECT shortage_id
    FROM shortages_with_ndc
    GROUP BY shortage_id
    HAVING COUNT(*) > 1
) duplicates;

--Date format sanity check verifies dates are stored as strings in the expected YYYYMMDD format.

SELECT
    'invalid_initial_posting_date' AS metric,
    COUNT(*) AS invalid_count
FROM shortages_with_ndc
WHERE initial_posting_date IS NOT NULL
  AND initial_posting_date <> ''
  AND initial_posting_date NOT REGEXP '^[0-9]{8}$';
  AND initial_posting_date NOT REGEXP '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}$';
SELECT
    'invalid_update_date' AS metric,
    COUNT(*) AS invalid_count
FROM shortages_with_ndc
WHERE update_date IS NOT NULL
  AND update_date <> ''
  AND update_date NOT REGEXP '^[0-9]{8}$';
  AND update_date NOT REGEXP '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}$';

-- Status value distribution check provides insight into the composition of shortage records by status.
SELECT
    'status_summary' AS metric,
    status,
    COUNT(*) AS row_count
FROM shortages_with_ndc
GROUP BY status
ORDER BY row_count DESC;

-- Sample of unmatched shortage records is useful for debugging join gaps.

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
