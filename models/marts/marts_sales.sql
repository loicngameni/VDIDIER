{{ config(materialized='table') }}

with sales as (
    select * from {{ ref('sales') }}
),
aggregated as (
    select
        s.product_id,
        s.product_name,
        s.brand_id,
        s.category_id,
        s.model_year,
        count(distinct s.order_id) as nb_orders,
        sum(s.quantity) as total_quantity_sold,
        round(sum(s.line_total), 2) as total_sales_amount,
        round(avg(s.list_price), 2) as avg_list_price,
        round(avg(s.discount) * 100, 2) as avg_discount_pct
    from sales s
    group by
        s.product_id,
        s.product_name,
        s.brand_id,
        s.category_id,
        s.model_year
)

select * from aggregated
