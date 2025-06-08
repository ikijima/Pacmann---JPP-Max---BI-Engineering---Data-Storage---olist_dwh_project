INSERT INTO dwh.dim_geolocation (
    geolocation_zip_code_prefix,
    geolocation_lat, geolocation_lng,
    geolocation_city, geolocation_state
)
SELECT geolocation_zip_code_prefix, 
  geolocation_lat, geolocation_lng, 
  geolocation_city, geolocation_state
FROM stg.geolocation
WHERE geolocation_zip_code_prefix IS NOT NULL
ON CONFLICT (geolocation_zip_code_prefix) DO NOTHING;