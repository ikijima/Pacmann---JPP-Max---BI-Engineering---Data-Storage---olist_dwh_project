INSERT INTO stg.product_category_name_translation 
    (product_category_name, 
    product_category_name_english) 
SELECT
    product_category_name, 
    product_category_name_english
FROM public.product_category_name_translation

ON CONFLICT(product_category_name) DO NOTHING;