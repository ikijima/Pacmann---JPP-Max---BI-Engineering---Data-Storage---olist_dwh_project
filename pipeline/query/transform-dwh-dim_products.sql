-- Step 1: Expire the current records with changes
UPDATE dwh.dim_products d
SET 
    record_end_date = CURRENT_DATE - INTERVAL '1 day',
    is_current = FALSE
FROM stg.products p
WHERE d.product_id = p.product_id
  AND d.is_current = TRUE
  AND (
      d.product_category_name IS DISTINCT FROM p.product_category_name OR
      d.product_name_length IS DISTINCT FROM p.product_name_length OR
      d.product_description_length IS DISTINCT FROM p.product_description_length OR
      d.product_photos_qty IS DISTINCT FROM p.product_photos_qty OR
      d.product_weight_g IS DISTINCT FROM p.product_weight_g OR
      d.product_length_cm IS DISTINCT FROM p.product_length_cm OR
      d.product_height_cm IS DISTINCT FROM p.product_height_cm OR
      d.product_width_cm IS DISTINCT FROM p.product_width_cm
  );

-- Step 2: Insert new records (new or changed)
INSERT INTO dwh.dim_products (
    product_id,
    product_category_name,
    product_name_length,
    product_description_length,
    product_photos_qty,
    product_weight_g,
    product_length_cm,
    product_height_cm,
    product_width_cm,
    record_start_date,
    is_current
)
SELECT DISTINCT
    p.product_id,
    p.product_category_name,
    p.product_name_length,
    p.product_description_length,
    p.product_photos_qty,
    p.product_weight_g,
    p.product_length_cm,
    p.product_height_cm,
    p.product_width_cm,
    CURRENT_DATE,
    TRUE
FROM stg.products p
LEFT JOIN dwh.dim_products d
  ON p.product_id = d.product_id AND d.is_current = TRUE
WHERE 
    d.product_id IS NULL -- New product
    OR (
      d.product_category_name IS DISTINCT FROM p.product_category_name OR
      d.product_name_length IS DISTINCT FROM p.product_name_length OR
      d.product_description_length IS DISTINCT FROM p.product_description_length OR
      d.product_photos_qty IS DISTINCT FROM p.product_photos_qty OR
      d.product_weight_g IS DISTINCT FROM p.product_weight_g OR
      d.product_length_cm IS DISTINCT FROM p.product_length_cm OR
      d.product_height_cm IS DISTINCT FROM p.product_height_cm OR
      d.product_width_cm IS DISTINCT FROM p.product_width_cm
    );
