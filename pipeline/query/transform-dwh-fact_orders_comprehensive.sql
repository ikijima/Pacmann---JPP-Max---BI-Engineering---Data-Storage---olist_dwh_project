INSERT INTO dwh.fact_orders_comprehensive (
    order_id, order_date_id, order_time_id,
    customer_sk, seller_sk, product_sk, product_category_sk,
    price, freight_value
)
SELECT
    oi.order_id, dd.full_date, dt.time_id,
    dc.customer_sk, ds.seller_sk, dp.product_sk, dpc.product_category_sk,
    oi.price, oi.freight_value

FROM stg.order_items oi
JOIN stg.orders o ON oi.order_id = o.order_id

JOIN dwh.dim_date dd ON dd.full_date = DATE(o.order_approved_at)
JOIN dwh.dim_time dt ON
    EXTRACT(HOUR FROM TO_TIMESTAMP(o.order_approved_at, 'YYYY-MM-DD HH24:MI:SS')) = dt.hour AND
    EXTRACT(MINUTE FROM TO_TIMESTAMP(o.order_approved_at, 'YYYY-MM-DD HH24:MI:SS')) = dt.minute AND
    EXTRACT(SECOND FROM TO_TIMESTAMP(o.order_approved_at, 'YYYY-MM-DD HH24:MI:SS')) = dt.second

JOIN dwh.dim_customers dc ON dc.customer_id = o.customer_id AND dc.is_current = TRUE
JOIN dwh.dim_sellers ds ON ds.seller_id = oi.seller_id AND ds.is_current = TRUE
JOIN dwh.dim_products dp ON dp.product_id = oi.product_id AND dp.is_current = TRUE
JOIN dwh.dim_product_category dpc ON dpc.product_category_name = dp.product_category_name;