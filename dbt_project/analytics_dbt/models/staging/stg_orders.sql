with source as (
    select *
    from {{ source('raw', 'orders') }}
),

valid_customers as (
    select customer_id
    from {{ ref('stg_customers') }}
),

cleaned as (
    select
        order_id,
        customer_id,
        nullif(trim(order_date), '')::date as order_date,
        order_amount::numeric(10,2) as order_amount,
        lower(trim(order_status)) as order_status
    from source
    where customer_id in (select customer_id from valid_customers)
)

select *
from cleaned
