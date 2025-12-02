-- sources
with orders as (
    select
        *
    from {{ref('stg_jaffle_shop_orders')}}   
),
customers as (
    select 
        *
    from {{ref('stg_jaffle_shop_customers')}}

),

payments as (
    select
        *
    from 
        {{ref('stg_stripe_payments')}}

),

-- logical
paid_orders as (
    select 
        *
    from
        {{ref('int_orders')}}    
),

amount_paid_by_order as (
    select
        p.order_id,
        sum(t2.total_amount_paid) as clv_bad
    from 
        paid_orders p
        left join paid_orders t2 on p.customer_id = t2.customer_id and p.order_id >= t2.order_id
    group by 1
    order by p.order_id
),

-- final
final as (
select
    p.*,
    row_number() over (order by p.order_id) as transaction_seq,
    row_number() over (partition by customer_id order by p.order_id) as customer_sales_seq,
    case 
        (when rank() over (partition by customer_id 
            order by order_placed_at, order_id)) = 1
        then 'new'
        else 'return' 
    end as nvsr,
    x.clv_bad as customer_lifetime_value,
    first_value(paid_orders.order_placed_at) over 
        (partition by paid_orders.customer_id 
        order by paid_orders.order_placed_at) as fdos
from 
    paid_orders p
    left join amount_paid_by_order x on x.order_id = p.order_id
    -- order by order_id
)


select 
    *
from 
    final
order by 
    order_id