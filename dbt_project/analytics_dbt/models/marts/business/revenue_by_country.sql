select
    c.country,
    sum(o.order_amount) as total_revenue,
    count(o.order_key)  as total_orders
from {{ ref('fact_orders') }} o
join {{ ref('dim_customers') }} c
  on o.customer_key = c.customer_key
group by c.country
