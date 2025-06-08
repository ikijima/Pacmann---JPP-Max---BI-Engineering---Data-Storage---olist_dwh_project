UPDATE dwh.dim_customers d
SET 
    is_current = FALSE,
    record_end_date = CURRENT_DATE
FROM stg.customers c
WHERE 
    d.customer_id = c.customer_id
    AND d.is_current = TRUE
    AND (
        d.customer_unique_id IS DISTINCT FROM c.customer_unique_id OR
        d.customer_zip_code_prefix IS DISTINCT FROM c.customer_zip_code_prefix OR
        d.customer_city IS DISTINCT FROM c.customer_city OR
        d.customer_state IS DISTINCT FROM c.customer_state
    );

INSERT INTO dwh.dim_customers (
    customer_id,
    customer_unique_id,
    customer_zip_code_prefix,
    customer_city,
    customer_state,
    record_start_date,
    is_current
)
SELECT
    c.customer_id,
    c.customer_unique_id,
    c.customer_zip_code_prefix,
    c.customer_city,
    c.customer_state,
    CURRENT_DATE,
    TRUE
FROM stg.customers c
LEFT JOIN dwh.dim_customers d
    ON c.customer_id = d.customer_id AND d.is_current = TRUE
WHERE 
    d.customer_id IS NULL OR (
        d.customer_unique_id IS DISTINCT FROM c.customer_unique_id OR
        d.customer_zip_code_prefix IS DISTINCT FROM c.customer_zip_code_prefix OR
        d.customer_city IS DISTINCT FROM c.customer_city OR
        d.customer_state IS DISTINCT FROM c.customer_state
    );
