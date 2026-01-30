with source as (
    select *
    from {{ source('raw', 'payments') }}
),

valid_orders as (
    select order_id
    from {{ ref('stg_orders') }}
),

cleaned as (
    select
        payment_id,
        order_id,
        nullif(trim(payment_date), '')::date as payment_date,
        amount_paid::numeric(10,2)           as amount_paid,
        lower(trim(payment_method))          as payment_method
    from source
    where order_id in (select order_id from valid_orders)
)

select *
from cleaned

