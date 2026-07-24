-- Databricks notebook source

CREATE OR REPLACE TABLE
restaurant_lakehouse.gold.d_sales_summary AS

SELECT
    DATE(o.timestamp) AS order_date,
    o.restaurant_id,
    r.name AS restaurant_name,
    r.city,

    COUNT(DISTINCT o.order_id) AS total_orders,

    COUNT(DISTINCT o.customer_id)
        AS unique_customers,

    ROUND(SUM(o.total_amount), 2)
        AS total_revenue,

    ROUND(AVG(o.total_amount), 2)
        AS average_order_value

FROM restaurant_lakehouse.silver.fact_orders o

LEFT JOIN restaurant_lakehouse.silver.dim_restaurants r
    ON o.restaurant_id = r.restaurant_id

WHERE o.total_amount IS NOT NULL

GROUP BY
    DATE(o.timestamp),
    o.restaurant_id,
    r.name,
    r.city;

-- COMMAND ----------


CREATE OR REPLACE TABLE
restaurant_lakehouse.gold.d_customer_360 AS

WITH order_stats AS (

    SELECT
        customer_id,
        COUNT(*) AS total_orders,
        ROUND(SUM(total_amount), 2) AS total_spend,
        ROUND(AVG(total_amount), 2) AS average_order_value,
        MAX(timestamp) AS last_order_timestamp

    FROM restaurant_lakehouse.silver.fact_orders

    GROUP BY customer_id
),

review_stats AS (

    SELECT
        customer_id,
        COUNT(*) AS review_count,
        ROUND(AVG(rating), 2) AS average_rating

    FROM restaurant_lakehouse.silver.fact_reviews

    GROUP BY customer_id
)

SELECT
    c.customer_id,
    c.name AS customer_name,
    c.email,
    c.city,
    c.join_date,

    COALESCE(o.total_orders, 0)
        AS total_orders,

    COALESCE(o.total_spend, 0)
        AS total_spend,

    COALESCE(o.average_order_value, 0)
        AS average_order_value,

    o.last_order_timestamp,

    COALESCE(r.review_count, 0)
        AS review_count,

    r.average_rating

FROM restaurant_lakehouse.silver.dim_customer c

LEFT JOIN order_stats o
    ON c.customer_id = o.customer_id

LEFT JOIN review_stats r
    ON c.customer_id = r.customer_id;

-- COMMAND ----------


CREATE OR REPLACE TABLE
restaurant_lakehouse.gold.d_restaurant_performance AS

WITH sales AS (

    SELECT
        restaurant_id,
        COUNT(*) AS total_orders,
        ROUND(SUM(total_amount), 2) AS total_revenue,
        ROUND(AVG(total_amount), 2) AS average_order_value

    FROM restaurant_lakehouse.silver.fact_orders

    GROUP BY restaurant_id
),

review_stats AS (

    SELECT
        restaurant_id,
        COUNT(*) AS total_reviews,
        ROUND(AVG(rating), 2) AS average_rating

    FROM restaurant_lakehouse.silver.fact_reviews

    GROUP BY restaurant_id
)

SELECT
    r.restaurant_id,
    r.name AS restaurant_name,
    r.city,
    r.country,

    COALESCE(s.total_orders, 0)
        AS total_orders,

    COALESCE(s.total_revenue, 0)
        AS total_revenue,

    COALESCE(s.average_order_value, 0)
        AS average_order_value,

    COALESCE(rv.total_reviews, 0)
        AS total_reviews,

    rv.average_rating

FROM restaurant_lakehouse.silver.dim_restaurants r

LEFT JOIN sales s
    ON r.restaurant_id = s.restaurant_id

LEFT JOIN review_stats rv
    ON r.restaurant_id = rv.restaurant_id;

-- COMMAND ----------


CREATE OR REPLACE TABLE
restaurant_lakehouse.gold.d_menu_performance AS

SELECT
    restaurant_id,
    item_id,
    item_name,
    category,

    SUM(quantity) AS units_sold,

    ROUND(
        SUM(subtotal),
        2
    ) AS total_revenue,

    ROUND(
        AVG(unit_price),
        2
    ) AS average_unit_price

FROM restaurant_lakehouse.silver.fact_order_items

GROUP BY
    restaurant_id,
    item_id,
    item_name,
    category;

-- COMMAND ----------


SHOW TABLES IN restaurant_lakehouse.gold;

-- COMMAND ----------

