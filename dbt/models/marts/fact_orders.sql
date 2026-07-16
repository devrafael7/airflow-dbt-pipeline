SELECT

    o.order_id,
    o.order_date,

    c.customer_id,
    c.full_name,
    c.region,

    p.product_id,
    p.product_name,
    p.price,

    o.quantity,

    p.price * o.quantity AS total_amount

FROM {{ ref('stg_orders') }} o

LEFT JOIN {{ ref('stg_customers') }} c
    ON o.customer_id = c.customer_id

LEFT JOIN {{ ref('stg_products') }} p
    ON o.product_id = p.product_id