select * from
{{ source('BRONZE', 'PRODUCTS') }}
