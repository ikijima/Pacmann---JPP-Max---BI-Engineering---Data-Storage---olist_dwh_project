CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE SCHEMA IF NOT EXISTS dwh AUTHORIZATION postgres;

CREATE TABLE dwh.dim_date (
    date_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    full_date TIMESTAMP UNIQUE NOT NULL,
    date_only DATE UNIQUE NOT NULL,
    year NUMERIC NOT NULL,
    month NUMERIC NOT NULL,
    day NUMERIC NOT NULL,
    week NUMERIC NOT NULL,
    quarter NUMERIC NOT NULL,
    semester NUMERIC NOT NULL,
    day_of_week VARCHAR(15) NOT NULL,
    month_name VARCHAR(15) NOT NULL,
    year_day VARCHAR(15) UNIQUE NOT NULL, --eg. 2025-02
    year_month VARCHAR(8) NOT NULL, -- e.g. '2025-06'
    year_week VARCHAR(8) NOT NULL, -- e.g. '2025-W23'
    year_quarter VARCHAR(8) NOT NULL, -- e.g. '2025-Q2'
    year_semester VARCHAR(8) NOT NULL -- e.g. '2025-S1'
);

CREATE TABLE dwh.dim_time (
    time_id TIME PRIMARY KEY,
    hour SMALLINT NOT NULL,
    minute SMALLINT NOT NULL,
    second SMALLINT NOT NULL
);

CREATE TABLE dwh.dim_geolocation (
    geolocation_sk SERIAL PRIMARY KEY,
    geolocation_zip_code_prefix INTEGER UNIQUE NOT NULL,
    geolocation_lat REAL,
    geolocation_lng REAL,
    geolocation_city TEXT,
    geolocation_state TEXT
);

CREATE TABLE dwh.dim_product_category (
    product_category_sk SERIAL PRIMARY KEY,
    product_category_name TEXT UNIQUE NOT NULL,
    product_category_name_english TEXT
);

CREATE TABLE dwh.dim_sellers (
    seller_sk SERIAL PRIMARY KEY,
    seller_id TEXT UNIQUE NOT NULL,
    seller_zip_code_prefix INTEGER,
    seller_city TEXT,
    seller_state TEXT,
    record_start_date DATE NOT NULL DEFAULT CURRENT_DATE,
    record_end_date DATE,
    is_current BOOLEAN NOT NULL DEFAULT TRUE
);

CREATE TABLE dwh.dim_customers (
    customer_sk SERIAL PRIMARY KEY,
    customer_id TEXT UNIQUE NOT NULL,
    customer_unique_id TEXT,
    customer_zip_code_prefix INTEGER,
    customer_city TEXT,
    customer_state TEXT,
    record_start_date DATE NOT NULL DEFAULT CURRENT_DATE,
    record_end_date DATE,
    is_current BOOLEAN NOT NULL DEFAULT TRUE
);

CREATE TABLE dwh.dim_products (
    product_sk SERIAL PRIMARY KEY,
    product_id TEXT NOT NULL,
    product_category_name TEXT REFERENCES dwh.dim_product_category(product_category_name),
    product_name_length REAL,
    product_description_length REAL,
    product_photos_qty REAL,
    product_weight_g REAL,
    product_length_cm REAL,
    product_height_cm REAL,
    product_width_cm REAL,
    record_start_date DATE NOT NULL DEFAULT CURRENT_DATE,
    record_end_date DATE,
    is_current BOOLEAN NOT NULL DEFAULT TRUE
);


CREATE TABLE dwh.fact_orders_comprehensive (
    order_compr_id SERIAL PRIMARY KEY, -- surrogate key

    order_id TEXT NOT NULL, -- natural key             
    order_date_id DATE NOT NULL REFERENCES dwh.dim_date(full_date),
    order_time_id TIME NOT NULL REFERENCES dwh.dim_time(time_id),
    customer_sk INTEGER NOT NULL REFERENCES dwh.dim_customers(customer_sk),
    seller_sk INTEGER NOT NULL REFERENCES dwh.dim_sellers(seller_sk),
    product_sk INTEGER NOT NULL REFERENCES dwh.dim_products(product_sk),
    product_category_sk INTEGER NOT NULL REFERENCES dwh.dim_product_category(product_category_sk),

    price NUMERIC NOT NULL,
    freight_value NUMERIC NOT NULL,
    total_value NUMERIC GENERATED ALWAYS AS (price + freight_value) STORED,

    inserted_at TIMESTAMP DEFAULT now()
);

CREATE TABLE dwh.fact_reviews_sentiment(
  review_id TEXT NOT NULL,

  order_compr_id INTEGER NOT NULL REFERENCES dwh.fact_orders_comprehensive(order_compr_id),
  customer_sk INTEGER NOT NULL REFERENCES dwh.dim_customers(customer_sk),
  product_sk INTEGER NOT NULL REFERENCES dwh.dim_products(product_sk),

  review_title TEXT,
  review_comment_message TEXT,
  review_score INTEGER
);

CREATE TABLE dwh.daily_periodical_snapshot_order_trends (
    daily_snapshot_id SERIAL PRIMARY KEY,
    order_date DATE UNIQUE REFERENCES dwh.dim_date(date_only),
    year_day VARCHAR NOT NULL,
    year_week VARCHAR NOT NULL,
    year_month VARCHAR NOT NULL,
    year_quarter VARCHAR NOT NULL,
    year_semester VARCHAR NOT NULL,
    distinct_customer_count INTEGER NOT NULL,
    distinct_seller_count INTEGER NOT NULL,
    order_id_count INTEGER NOT NULL,
    product_id_count INTEGER NOT NULL,
    sum_price NUMERIC NOT NULL,
    sum_freight_value NUMERIC NOT NULL,
    inserted_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE dwh.accumulating_snapshot_logistic_performance (
    accm_order_id SERIAL PRIMARY KEY,
    order_id TEXT,
    product_sk INTEGER NOT NULL REFERENCES dwh.dim_products(product_sk),
    seller_sk INTEGER NOT NULL REFERENCES dwh.dim_sellers(seller_sk),
    customer_sk INTEGER NOT NULL REFERENCES dwh.dim_customers(customer_sk),

    order_purchase_timestamp TIMESTAMP NOT NULL,
    order_approved_at TIMESTAMP,
    purchase_approve_difference INTERVAL,

    order_estimated_delivery_date TIMESTAMP,
    order_shipping_limit_date TIMESTAMP,
    order_delivered_carrier_date TIMESTAMP,
    order_delivered_customer_date TIMESTAMP,

    approved_carrier_difference INTERVAL,
    approved_delivered_difference INTERVAL,

    order_purchase_date_id UUID REFERENCES dwh.dim_date(date_id),
    order_delivered_date_id UUID REFERENCES dwh.dim_date(date_id),

    last_updated TIMESTAMP NOT NULL DEFAULT NOW(),

    order_status_code VARCHAR
);