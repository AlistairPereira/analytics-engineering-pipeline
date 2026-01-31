select 
row_number() over (order by o.order_id) as order_key,
c.customer_key,
    o.order_id,
    o.order_date,
    o.order_amount,
    o.order_status
FROM
{{ ref('stg_orders') }} as o 
join {{ ref('dim_customers') }} as c 
on o.customer_id = c.customer_id