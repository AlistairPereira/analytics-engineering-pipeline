with orders as (

    select
        customer_key,
        order_date,
        order_amount
    from {{ ref('fact_orders') }}
    where customer_key is not null

),

agg as (

    select
        customer_key,
        count(*) as order_count,
        sum(order_amount) as total_revenue,
        min(order_date) as first_order_date,
        max(order_date) as last_order_date
    from orders
    group by customer_key

),

customers as (

    select
        customer_key,
        customer_id,
        customer_name,
        country
    from {{ ref('dim_customers') }}

)

select
    a.customer_key,
    c.customer_id,
    c.customer_name,
    c.country,
    a.order_count,
    a.total_revenue,
    a.first_order_date,
    a.last_order_date
from agg a
left join customers c
    on a.customer_key = c.customer_key
order by a.total_revenue desc
