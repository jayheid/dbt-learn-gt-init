select
    customer_id,
    order_date,
    {{ dbt_utils.generate_surrogate_key(['customer_id', 'order_date']) }} as pk,
    count(*) as c
from {{ ref('stg_jaffle_shop_orders') }}
group by 1,2
