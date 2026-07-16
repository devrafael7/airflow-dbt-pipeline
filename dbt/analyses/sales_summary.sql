SELECT
    category_name,
    COUNT(*) AS total_orders,
    SUM(quantity) AS total_items,
    SUM(price * quantity) AS total_sales

FROM {{ ref('fact_orders') }}

GROUP BY product_name

ORDER BY total_sales DESC;