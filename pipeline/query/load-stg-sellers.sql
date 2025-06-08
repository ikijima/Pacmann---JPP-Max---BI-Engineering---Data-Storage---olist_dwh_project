INSERT INTO stg.sellers (
  seller_id, seller_zip_code_prefix, seller_city, seller_state
)
SELECT
  seller_id, seller_zip_code_prefix, seller_city, seller_state
FROM public.sellers

ON CONFLICT(seller_id) DO NOTHING;