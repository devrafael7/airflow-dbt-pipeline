SELECT
    c.customer_id,
    c.full_name,
    {{ clean_email('c.email') }} AS email,
    c.state,
    c.city,
    s.region

FROM {{ source('BRONZE', 'CUSTOMERS') }} AS c

LEFT JOIN {{ ref('states') }} AS s
    ON c.state = s.state