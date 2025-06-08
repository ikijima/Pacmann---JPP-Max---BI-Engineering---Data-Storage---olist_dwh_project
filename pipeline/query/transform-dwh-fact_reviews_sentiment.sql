INSERT INTO dwh.fact_reviews_sentiment (
    review_id,
    order_compr_id,
    customer_sk,
    product_sk,
    review_title,
    review_comment_message,
    review_score
)
SELECT
    r.review_id,
    foc.order_compr_id,
    foc.customer_sk,
    foc.product_sk,
    r.review_comment_title,
    r.review_comment_message,
    r.review_score

FROM stg.order_reviews r
join dwh.fact_orders_comprehensive foc on r.order_id = foc.order_id
JOIN stg.orders o ON r.order_id = o.order_id

