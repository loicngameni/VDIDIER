
with
orders as (
    select * from {{ ref('stg_orders') }}
),
order_items as (
    select * from {{ ref('stg_order_items') }}
),
products as (
    select * from {{ ref('stg_products') }}
)

select
    o.order_id,
    o.customer_id,
    o.order_date,
    o.shipped_date,
    o.order_status,
    oi.item_id,
    oi.product_id,
    p.product_name,
    p.brand_id,
    p.category_id,
    p.model_year,
    oi.quantity,
    oi.list_price,
    oi.discount,
    oi.line_total,
    round(sum(oi.line_total) over (partition by o.order_id), 2) as order_total
from order_items oi
join orders o on oi.order_id = o.order_id
join products p on oi.product_id = p.product_id

