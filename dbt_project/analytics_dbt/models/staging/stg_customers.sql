with source as (
    select *
    from {{ source('raw', 'customers') }}
),

cleaned as (
    select
        customer_id,
        trim(customer_name) as customer_name,
        lower(trim(email)) as email,
        nullif(trim(signup_date), '')::date as signup_date,
        upper(trim(country)) as country
    from source
    where email is not null
)

select *
from cleaned
