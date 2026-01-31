select 
row_number() over (order by customer_id) as customer_key,
customer_id,
customer_name,
email,
signup_date,
country
 from  {{ ref('stg_customers') }}

 