-- logical
with 
paid_orders as (
    select 
        *
    from
        {{ref('int_orders')}}    
),
-- final
final as (
select
    customer_id,
    order_placed_at,
    order_status,
    total_amount_paid,
    payment_finalized_date,
    customer_first_name,
    customer_last_name,
    row_number() over (order by order_id) as transaction_seq,
    row_number() over (partition by customer_id order by order_id) as customer_sales_seq,
    case 
        when rank() over (partition by customer_id 
            order by order_placed_at, order_id) = 1
        then 'new'
        else 'return' 
    end as nvsr,
    sum(paid_orders.total_amount_paid) 
        over (partition by paid_orders.order_id 
        order by paid_orders.order_placed_at, paid_orders.order_id) as customer_lifetime_value,
    first_value(paid_orders.order_placed_at) over 
        (partition by paid_orders.customer_id 
        order by paid_orders.order_placed_at) as fdos
from 
    paid_orders
)


select 
    *
from 
    final