-- Step 1: Expire old records if there's a change
UPDATE dwh.dim_sellers d
SET 
    record_end_date = CURRENT_DATE - INTERVAL '1 day',
    is_current = FALSE
FROM stg.sellers s
WHERE d.seller_id = s.seller_id
  AND d.is_current = TRUE
  AND (
        d.seller_zip_code_prefix IS DISTINCT FROM s.seller_zip_code_prefix OR
        d.seller_city IS DISTINCT FROM s.seller_city OR
        d.seller_state IS DISTINCT FROM s.seller_state
      );

-- Step 2: Insert new records (only new or changed)
INSERT INTO dwh.dim_sellers (
    seller_id,
    seller_zip_code_prefix,
    seller_city,
    seller_state,
    record_start_date,
    is_current
)
SELECT
    s.seller_id,
    s.seller_zip_code_prefix,
    s.seller_city,
    s.seller_state,
    CURRENT_DATE,
    TRUE
FROM stg.sellers s
LEFT JOIN dwh.dim_sellers d
  ON s.seller_id = d.seller_id AND d.is_current = TRUE
WHERE 
    d.seller_id IS NULL -- new
    OR (
        d.seller_zip_code_prefix IS DISTINCT FROM s.seller_zip_code_prefix OR
        d.seller_city IS DISTINCT FROM s.seller_city OR
        d.seller_state IS DISTINCT FROM s.seller_state
    );
