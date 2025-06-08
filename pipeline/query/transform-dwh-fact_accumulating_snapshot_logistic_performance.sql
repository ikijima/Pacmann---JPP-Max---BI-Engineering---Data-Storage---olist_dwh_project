INSERT INTO dwh.accumulating_snapshot_logistic_performance (
    order_id,
    product_sk,
    seller_sk,
    customer_sk,
    order_purchase_timestamp,
    order_approved_at,
    purchase_approve_difference,
    order_estimated_delivery_date,
    order_shipping_limit_date,
    order_delivered_carrier_date,
    order_delivered_customer_date,
    approved_carrier_difference,
    approved_delivered_difference,
    order_purchase_date_id,
    order_delivered_date_id,
    last_updated,
    order_status_code
)

SELECT
    o.order_id,
    dp.product_sk,
    ds.seller_sk,
    dc.customer_sk,

    TO_TIMESTAMP(o.order_purchase_timestamp, 'YYYY-MM-DD HH24:MI:SS') AS order_purchase_ts,
    TO_TIMESTAMP(o.order_approved_at, 'YYYY-MM-DD HH24:MI:SS') AS approved_ts,

    CASE 
        WHEN o.order_approved_at IS NOT NULL 
        THEN TO_TIMESTAMP(o.order_approved_at, 'YYYY-MM-DD HH24:MI:SS') - TO_TIMESTAMP(o.order_purchase_timestamp, 'YYYY-MM-DD HH24:MI:SS')
        ELSE NULL
    END AS purchase_approve_diff,

    TO_TIMESTAMP(o.order_estimated_delivery_date, 'YYYY-MM-DD') AS estimated_delivery,
    TO_TIMESTAMP(oi.shipping_limit_date, 'YYYY-MM-DD HH24:MI:SS') AS shipping_limit,
    TO_TIMESTAMP(o.order_delivered_carrier_date, 'YYYY-MM-DD HH24:MI:SS') AS delivered_carrier,
    TO_TIMESTAMP(o.order_delivered_customer_date, 'YYYY-MM-DD HH24:MI:SS') AS delivered_customer,

    CASE 
        WHEN o.order_delivered_carrier_date IS NOT NULL AND o.order_approved_at IS NOT NULL
        THEN TO_TIMESTAMP(o.order_delivered_carrier_date, 'YYYY-MM-DD HH24:MI:SS') - TO_TIMESTAMP(o.order_approved_at, 'YYYY-MM-DD HH24:MI:SS')
        ELSE NULL
    END AS approved_carrier_diff,

    CASE 
        WHEN o.order_delivered_customer_date IS NOT NULL AND o.order_approved_at IS NOT NULL
        THEN TO_TIMESTAMP(o.order_delivered_customer_date, 'YYYY-MM-DD HH24:MI:SS') - TO_TIMESTAMP(o.order_approved_at, 'YYYY-MM-DD HH24:MI:SS')
        ELSE NULL
    END AS approved_delivered_diff,

    dd_order.date_id AS order_purchase_date_id,
    dd_delivered.date_id AS order_delivered_date_id,

    NOW() AS last_updated,

    o.order_status AS order_status_code

FROM stg.orders o
JOIN stg.order_items oi ON o.order_id = oi.order_id

-- Dimension lookups with SCD type 2 handling
JOIN dwh.dim_customers dc ON o.customer_id = dc.customer_id AND dc.is_current = TRUE
JOIN dwh.dim_sellers ds ON oi.seller_id = ds.seller_id AND ds.is_current = TRUE
JOIN dwh.dim_products dp ON oi.product_id = dp.product_id

-- Dates
LEFT JOIN dwh.dim_date dd_order ON dd_order.full_date = TO_DATE(o.order_purchase_timestamp, 'YYYY-MM-DD')
LEFT JOIN dwh.dim_date dd_delivered ON dd_delivered.full_date = TO_DATE(o.order_delivered_customer_date, 'YYYY-MM-DD')

-- Only if order_purchase_timestamp is present
WHERE o.order_purchase_timestamp IS NOT NULL;
