{{ config(materialized='view') }}

with
customers as (
    select * from {{ ref('stg_customers') }}
),
orders as (
    select * from {{ ref('stg_orders') }}
),
sales as (
    select * from {{ ref('sales') }}
)

select
    c.customer_id,
    c.first_name,
    c.last_name,
    c.email,
    c.city,
    c.state,

    o.order_id,
    o.order_date,
    o.shipped_date,
    o.order_status,

    round(sum(s.line_total) over (partition by o.order_id), 2) as order_total,
    round(sum(s.line_total) over (partition by c.customer_id), 2) as total_customer_sales,
    count(distinct o.order_id) over (partition by c.customer_id) as total_orders

from customers c
left join orders o on o.customer_id = c.customer_id
left join sales s on s.order_id = o.order_id

