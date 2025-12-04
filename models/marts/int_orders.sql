with orders as (
    select
        *
    from {{ref('stg_jaffle_shop_orders')}}   
),

payments as (
    select
        *
    from 
        {{ref('stg_stripe_payments')}}

),
customers as (
    select
        customer_id,
        first_name as customer_first_name,
        last_name as customer_last_name
    from 
        {{ref('stg_jaffle_shop_customers')}}
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
        completed_payments.total_amount_paid,
        completed_payments.payment_finalized_date,
        customers.customer_first_name,
        customers.customer_last_name
    from orders
    left join completed_payments on orders.order_id = completed_payments.order_id
    left join customers on orders.customer_id = customers.customer_id
)

select 
    * 
from 
    paid_orders