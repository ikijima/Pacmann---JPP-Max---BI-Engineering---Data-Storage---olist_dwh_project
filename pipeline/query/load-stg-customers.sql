INSERT INTO stg.customers (
  customer_id, customer_unique_id, customer_zip_code_prefix, customer_city, customer_state
)
SELECT customer_id, customer_unique_id, customer_zip_code_prefix, customer_city, customer_state
FROM public.customers

ON CONFLICT(customer_id) DO NOTHING;