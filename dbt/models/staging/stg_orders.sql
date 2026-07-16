SELECT
    ord.*,
    prod.product_name,
    prod.price,
    cat.category_name

FROM {{ source('BRONZE', 'ORDERS') }} AS ord

INNER JOIN {{ source('BRONZE', 'PRODUCTS') }} AS prod
    ON ord.product_id = prod.product_id

INNER JOIN {{ ref('categories') }} AS cat
    ON prod.category_id = cat.category_id