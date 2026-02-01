--This file verifies that ETL pipeline is executed succesfully.Basically monitors the health of pipeline.
--This checks if
--tables exist
--tables contains data
--transformation is producing proper results

USE fda_shortage_db;

-- ensure table exist

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


-- Confirms that tables are not empty after loading

SELECT 'raw_ndc' AS table_name, COUNT(*) AS row_count FROM raw_ndc
UNION ALL
SELECT 'raw_ndc_packaging', COUNT(*) FROM raw_ndc_packaging
UNION ALL
SELECT 'raw_drug_shortages', COUNT(*) FROM raw_drug_shortages
UNION ALL
SELECT 'shortages_with_ndc', COUNT(*) FROM shortages_with_ndc;



-- Confirms that the joined table contains data

SELECT
    'shortages_with_ndc_status' AS check_name,
    CASE
        WHEN COUNT(*) > 0 THEN 'PASS'
        ELSE 'FAIL'
    END AS result,
    COUNT(*) AS row_count
FROM shortages_with_ndc;


-- Checks most recent update_date available

SELECT
    'latest_update_date' AS metric,
    MAX(update_date) AS most_recent_update
FROM shortages_with_ndc;


-- Confirms analytical views were created successfully

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
