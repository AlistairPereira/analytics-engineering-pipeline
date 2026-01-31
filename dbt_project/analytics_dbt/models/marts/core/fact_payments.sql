select
    row_number() over (order by p.payment_id) as payment_key,
    o.order_key,
    p.payment_id,
    p.payment_date,
    p.amount_paid,
    p.payment_method
from {{ ref('stg_payments') }} p
join {{ ref('fact_orders') }} o
  on p.order_id = o.order_id
