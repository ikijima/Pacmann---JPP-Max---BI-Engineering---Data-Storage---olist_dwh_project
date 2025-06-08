CREATE TABLE IF NOT EXISTS stg.geolocation (
    geolocation_zip_code_prefix INTEGER UNIQUE NOT NULL,
    geolocation_lat REAL,
    geolocation_lng REAL,
    geolocation_city TEXT,
    geolocation_state TEXT
);

CREATE TABLE IF NOT EXISTS stg.product_category_name_translation (
    product_category_name TEXT UNIQUE NOT NULL,
    product_category_name_english TEXT
);

CREATE TABLE IF NOT EXISTS stg.sellers (
    seller_id TEXT UNIQUE NOT NULL,
    seller_zip_code_prefix INTEGER,
    seller_city TEXT,
    seller_state TEXT
);

CREATE TABLE IF NOT EXISTS stg.customers (
    customer_id TEXT UNIQUE NOT NULL,
    customer_unique_id TEXT,
    customer_zip_code_prefix INTEGER,
    customer_city TEXT,
    customer_state TEXT
);

CREATE TABLE IF NOT EXISTS stg.products (
    product_id TEXT UNIQUE NOT NULL,
    product_category_name TEXT REFERENCES stg.product_category_name_translation(product_category_name),
    product_name_length REAL,
    product_description_length REAL,
    product_photos_qty REAL,
    product_weight_g REAL,
    product_length_cm REAL,
    product_height_cm REAL,
    product_width_cm REAL
);

CREATE TABLE IF NOT EXISTS stg.orders (
    order_id TEXT UNIQUE NOT NULL,
    customer_id TEXT REFERENCES stg.customers(customer_id),
    order_status TEXT,
    order_purchase_timestamp TEXT,
    order_approved_at TEXT,
    order_delivered_carrier_date TEXT,
    order_delivered_customer_date TEXT,
    order_estimated_delivery_date TEXT
);

CREATE TABLE IF NOT EXISTS stg.order_items (
    order_id TEXT NOT NULL REFERENCES stg.orders(order_id),
    order_item_id INTEGER NOT NULL,
    product_id TEXT REFERENCES stg.products(product_id),
    seller_id TEXT REFERENCES stg.sellers(seller_id),
    shipping_limit_date TEXT,
    price NUMERIC,
    freight_value NUMERIC
);

CREATE TABLE IF NOT EXISTS stg.order_payments (
    order_id TEXT NOT NULL,
    payment_sequential INTEGER NOT NULL,
    payment_type TEXT,
    payment_installments INTEGER,
    payment_value NUMERIC
);

CREATE TABLE IF NOT EXISTS stg.order_reviews (
    review_id text UNIQUE NOT NULL,
    order_id text NOT NULL REFERENCES stg.orders(order_id),
    review_score integer,
    review_comment_title text,
    review_comment_message text,
    review_creation_date text
);