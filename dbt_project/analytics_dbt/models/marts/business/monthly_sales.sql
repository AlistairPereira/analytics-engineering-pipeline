with orders as (

    select
        order_date,
        order_amount
    from {{ ref('fact_orders') }}
    where order_date is not null

),

monthly as (

    select
        date_trunc('month', order_date)::date as month,
        count(*) as total_orders,
        sum(order_amount) as total_revenue
    from orders
    group by month

)

select *
from monthly
order by month
