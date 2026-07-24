-- Databricks notebook source

SELECT order_id, COUNT(*) AS count
FROM restaurant_lakehouse.silver.fact_orders
GROUP BY order_id
HAVING COUNT(*) > 1;

-- COMMAND ----------


SELECT customer_id, COUNT(*)
FROM restaurant_lakehouse.silver.dim_customer
GROUP BY customer_id
HAVING COUNT(*) > 1;

-- COMMAND ----------


SELECT COUNT(*) AS orphan_orders
FROM restaurant_lakehouse.silver.fact_orders o

LEFT JOIN restaurant_lakehouse.silver.dim_customer c
    ON o.customer_id = c.customer_id

WHERE c.customer_id IS NULL;

-- COMMAND ----------


SELECT COUNT(*) AS orphan_restaurants
FROM restaurant_lakehouse.silver.fact_orders o

LEFT JOIN restaurant_lakehouse.silver.dim_restaurants r
    ON o.restaurant_id = r.restaurant_id

WHERE r.restaurant_id IS NULL;

-- COMMAND ----------


SELECT
    COUNT(*) AS total_orders,

    SUM(
        CASE WHEN customer_id IS NULL
        THEN 1 ELSE 0 END
    ) AS missing_customers,

    SUM(
        CASE WHEN restaurant_id IS NULL
        THEN 1 ELSE 0 END
    ) AS missing_restaurants,

    SUM(
        CASE WHEN total_amount IS NULL
        THEN 1 ELSE 0 END
    ) AS missing_amounts

FROM restaurant_lakehouse.silver.fact_orders;

-- COMMAND ----------


SELECT *
FROM restaurant_lakehouse.silver.fact_reviews
WHERE rating < 1 OR rating > 5;

-- COMMAND ----------

