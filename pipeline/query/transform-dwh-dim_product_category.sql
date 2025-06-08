INSERT INTO dwh.dim_product_category (
    product_category_name,
    product_category_name_english
    )
 SELECT DISTINCT
    product_category_name,
    product_category_name_english
 FROM stg.product_category_name_translation
 WHERE product_category_name IS NOT NULL
 ON CONFLICT(product_category_name) DO NOTHING;