{{ config(materialized='table') }}

with customers_orders as (
    select * from {{ ref('customers_orders') }}
),
sales as (
    select * from {{ ref('sales') }}
),
joined as (
    select
        co.customer_id,
        co.email,
        co.city,
        co.state,
        s.order_id,
        s.line_total
    from customers_orders co
    left join sales s on co.order_id = s.order_id
),
aggregated as (
    select
        customer_id,
        email,
        city,
        state,
        count(distinct order_id) as nb_orders,
        round(sum(line_total), 2) as total_sales_amount,
        round(avg(line_total), 2) as avg_order_value
    from joined
    group by
        customer_id,
        email,
        city,
        state
)

select * from aggregated
