INSERT INTO stg.geolocation
    (geolocation_zip_code_prefix,
    geolocation_lat,
    geolocation_lng,
    geolocation_city,
    geolocation_state) 
SELECT
    geolocation_zip_code_prefix, 
    geolocation_lat,
    geolocation_lng,
    geolocation_city,
    geolocation_state
FROM public.geolocation

ON CONFLICT (geolocation_zip_code_prefix) DO NOTHING;