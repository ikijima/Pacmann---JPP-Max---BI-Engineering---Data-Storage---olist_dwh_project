INSERT INTO dwh.daily_periodical_snapshot_order_trends (
    order_date,
    year_day,
    year_week,
    year_month,
    year_quarter,
    year_semester,
    distinct_customer_count,
    distinct_seller_count,
    order_id_count,
    product_id_count,
    sum_price,
    sum_freight_value
)
SELECT
    dd.date_only AS order_date,
    dd.year_day,
    dd.year_week,
    dd.year_month,
    dd.year_quarter,
    dd.year_semester,

    COUNT(DISTINCT o.customer_id) AS distinct_customer_count,
    COUNT(DISTINCT oi.seller_id) AS distinct_seller_count,
    COUNT(DISTINCT o.order_id) AS order_id_count,
    COUNT(DISTINCT oi.product_id) AS product_id_count,
    SUM(oi.price) AS sum_price,
    SUM(oi.freight_value) AS sum_freight_value

FROM stg.orders o
JOIN stg.order_items oi ON oi.order_id = o.order_id
JOIN dwh.dim_date dd ON dd.full_date = TO_TIMESTAMP(o.order_approved_at, 'YYYY-MM-DD HH24:MI:SS')::DATE

-- Optional: avoid duplicate snapshot inserts
LEFT JOIN dwh.daily_periodical_snapshot_order_trends EXISTING
    ON existing.order_date = dd.full_date
WHERE o.order_approved_at IS NOT NULL
  AND existing.order_date IS NULL

GROUP BY
    dd.date_only,
    dd.year_day,
    dd.year_week,
    dd.year_month,
    dd.year_quarter,
    dd.year_semester;
