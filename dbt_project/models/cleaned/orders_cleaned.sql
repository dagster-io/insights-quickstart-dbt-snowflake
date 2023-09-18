select
        user_id,
        quantity,
        purchase_price,
        sku,
        dt,
        TO_VARCHAR(TO_DATE(dt)) as date,
        quantity * purchase_price as order_total
from {{ source('raw_data', 'orders') }}