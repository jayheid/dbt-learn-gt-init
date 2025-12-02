-- sources
with orders as (
    select
        *
    from {{source('jaffle_shop', 'orders')}}   
),
customers as (
    select 
        *
    from {{source('jaffle_shop', 'orders')}}

),

payments as (
    select
        *
    from 
        {{source('stripe', 'payments')}}

),

-- logical
completed_payments as (
    select 
        order_id, 
        max(created_at) as payment_finalized_date, 
        sum(amount) / 100.0 as total_amount_paid
    from 
        payments
    where 
        status <> 'fail'
    group by 1
),
paid_orders as (
    select 
        orders.order_id,
        orders.customer_id,
        orders.order_date as order_placed_at,
        orders.status as order_status,
        p.total_amount_paid,
        p.payment_finalized_date,
    from orders
    left join completed_payments p on orders.order_id = p.order_id
    left join customers c on orders.customer_id = c.customer_id ),

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