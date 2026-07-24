-- Databricks notebook source
-- MAGIC %python
-- MAGIC from pyspark.sql import functions as F
-- MAGIC from pyspark.sql.types import *

-- COMMAND ----------

-- MAGIC %python
-- MAGIC customers = spark.table("restaurant_lakehouse.bronze.customers")
-- MAGIC restaurants = spark.table("restaurant_lakehouse.bronze.restaurants")
-- MAGIC menu_items = spark.table("restaurant_lakehouse.bronze.menu_items")
-- MAGIC orders = spark.table("restaurant_lakehouse.bronze.historical_orders")
-- MAGIC reviews = spark.table("restaurant_lakehouse.bronze.reviews")
-- MAGIC
-- MAGIC print("Bronze tables loaded successfully")
-- MAGIC print("Customers:", customers.count())
-- MAGIC print("Restaurants:", restaurants.count())
-- MAGIC print("Menu Items:", menu_items.count())
-- MAGIC print("Orders:", orders.count())
-- MAGIC print("Reviews:", reviews.count())

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dim_customer = (
-- MAGIC     customers
-- MAGIC     .filter(F.col("customer_id").isNotNull())
-- MAGIC     .withColumn("name", F.trim(F.col("name")))
-- MAGIC     .withColumn("email", F.lower(F.trim(F.col("email"))))
-- MAGIC     .withColumn("city", F.trim(F.col("city")))
-- MAGIC     .withColumn("phone", F.col("phone").cast("string"))
-- MAGIC     .dropDuplicates(["customer_id"])
-- MAGIC )
-- MAGIC (
-- MAGIC     dim_customer.write
-- MAGIC     .format("delta")
-- MAGIC     .mode("overwrite")
-- MAGIC     .saveAsTable("restaurant_lakehouse.silver.dim_customer")
-- MAGIC )

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dim_menu_items = (
-- MAGIC     menu_items
-- MAGIC     .filter(
-- MAGIC         F.col("restaurant_id").isNotNull() &
-- MAGIC         F.col("item_id").isNotNull()
-- MAGIC     )
-- MAGIC     .withColumn("name", F.trim(F.col("name")))
-- MAGIC     .withColumn("category", F.trim(F.col("category")))
-- MAGIC     .withColumn("price", F.col("price").cast("double"))
-- MAGIC     .withColumn("spice_level", F.trim(F.col("spice_level")))
-- MAGIC     .dropDuplicates(["restaurant_id", "item_id"])
-- MAGIC )
-- MAGIC (
-- MAGIC     dim_menu_items.write
-- MAGIC     .format("delta")
-- MAGIC     .mode("overwrite")
-- MAGIC     .saveAsTable("restaurant_lakehouse.silver.dim_menu_items")
-- MAGIC )

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dim_restaurants = (
-- MAGIC     restaurants
-- MAGIC     .filter(F.col("restaurant_id").isNotNull())
-- MAGIC     .withColumn("name", F.trim(F.col("name")))
-- MAGIC     .withColumn("city", F.trim(F.col("city")))
-- MAGIC     .withColumn("country", F.trim(F.col("country")))
-- MAGIC     .withColumn("address", F.trim(F.col("address")))
-- MAGIC     .withColumn("phone", F.trim(F.col("phone")))
-- MAGIC     .dropDuplicates(["restaurant_id"])
-- MAGIC )
-- MAGIC (
-- MAGIC     dim_restaurants.write
-- MAGIC     .format("delta")
-- MAGIC     .mode("overwrite")
-- MAGIC     .saveAsTable("restaurant_lakehouse.silver.dim_restaurants")
-- MAGIC )

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(
-- MAGIC     orders.select(
-- MAGIC         "order_id",
-- MAGIC         "items",
-- MAGIC         "total_amount",
-- MAGIC         "created_at"
-- MAGIC     ).limit(10)
-- MAGIC )

-- COMMAND ----------

-- MAGIC %python
-- MAGIC sample_items = (
-- MAGIC     orders
-- MAGIC     .select("items")
-- MAGIC     .filter(F.col("items").isNotNull())
-- MAGIC     .limit(5)
-- MAGIC     .collect()
-- MAGIC )
-- MAGIC
-- MAGIC for row in sample_items:
-- MAGIC     print(row["items"])

-- COMMAND ----------

-- MAGIC %python
-- MAGIC orders_clean = (
-- MAGIC     orders
-- MAGIC     .filter(F.col("order_id").isNotNull())
-- MAGIC     .withColumn(
-- MAGIC         "total_amount",
-- MAGIC         F.regexp_replace(
-- MAGIC             F.col("total_amount"),
-- MAGIC             r"[^0-9.-]",
-- MAGIC             ""
-- MAGIC         ).cast("double")
-- MAGIC     )
-- MAGIC     .withColumn(
-- MAGIC         "created_at",
-- MAGIC         F.to_timestamp(F.col("created_at"))
-- MAGIC     )
-- MAGIC     .withColumn(
-- MAGIC         "order_type",
-- MAGIC         F.trim(F.col("order_type"))
-- MAGIC     )
-- MAGIC     .withColumn(
-- MAGIC         "payment_method",
-- MAGIC         F.trim(F.col("payment_method"))
-- MAGIC     )
-- MAGIC     .withColumn(
-- MAGIC         "order_status",
-- MAGIC         F.trim(F.col("order_status"))
-- MAGIC     )
-- MAGIC     .dropDuplicates(["order_id"])
-- MAGIC )

-- COMMAND ----------

-- MAGIC %python
-- MAGIC fact_orders = orders_clean.select(
-- MAGIC     "order_id",
-- MAGIC     "timestamp",
-- MAGIC     "restaurant_id",
-- MAGIC     "customer_id",
-- MAGIC     "order_type",
-- MAGIC     "total_amount",
-- MAGIC     "payment_method",
-- MAGIC     "order_status",
-- MAGIC     "created_at"
-- MAGIC )

-- COMMAND ----------

-- MAGIC %python
-- MAGIC orders_clean = (
-- MAGIC     orders
-- MAGIC     .filter(F.col("order_id").isNotNull())
-- MAGIC     .withColumn(
-- MAGIC         "total_amount_clean",
-- MAGIC         F.regexp_replace(
-- MAGIC             F.col("total_amount"),
-- MAGIC             r"[^0-9.-]",
-- MAGIC             ""
-- MAGIC         )
-- MAGIC     )
-- MAGIC     .withColumn(
-- MAGIC         "total_amount",
-- MAGIC         F.expr("try_cast(total_amount_clean AS DOUBLE)")
-- MAGIC     )
-- MAGIC     .drop("total_amount_clean")
-- MAGIC     .withColumn(
-- MAGIC         "created_at",
-- MAGIC         F.expr("try_cast(created_at AS TIMESTAMP)")
-- MAGIC     )
-- MAGIC     .withColumn(
-- MAGIC         "order_type",
-- MAGIC         F.trim(F.col("order_type"))
-- MAGIC     )
-- MAGIC     .withColumn(
-- MAGIC         "payment_method",
-- MAGIC         F.trim(F.col("payment_method"))
-- MAGIC     )
-- MAGIC     .withColumn(
-- MAGIC         "order_status",
-- MAGIC         F.trim(F.col("order_status"))
-- MAGIC     )
-- MAGIC     .dropDuplicates(["order_id"])
-- MAGIC )

-- COMMAND ----------

-- MAGIC %python
-- MAGIC bad_total_amount = orders_clean.filter(
-- MAGIC     F.col("total_amount").isNull()
-- MAGIC ).count()
-- MAGIC
-- MAGIC print("Invalid/null total_amount rows:", bad_total_amount)
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC fact_orders = orders_clean.select(
-- MAGIC     "order_id",
-- MAGIC     "timestamp",
-- MAGIC     "restaurant_id",
-- MAGIC     "customer_id",
-- MAGIC     "order_type",
-- MAGIC     "total_amount",
-- MAGIC     "payment_method",
-- MAGIC     "order_status",
-- MAGIC     "created_at"
-- MAGIC )
-- MAGIC (
-- MAGIC     fact_orders.write
-- MAGIC     .format("delta")
-- MAGIC     .mode("overwrite")
-- MAGIC     .saveAsTable("restaurant_lakehouse.silver.fact_orders")
-- MAGIC )

-- COMMAND ----------

-- MAGIC %python
-- MAGIC fact_reviews = (
-- MAGIC     reviews
-- MAGIC     .filter(F.col("review_id").isNotNull())
-- MAGIC     .withColumn("review_text", F.trim(F.col("review_text")))
-- MAGIC     .withColumn("rating", F.col("rating").cast("int"))
-- MAGIC     .dropDuplicates(["review_id"])
-- MAGIC )
-- MAGIC fact_reviews = fact_reviews.filter(
-- MAGIC     F.col("rating").between(1, 5)
-- MAGIC )
-- MAGIC (
-- MAGIC     fact_reviews.write
-- MAGIC     .format("delta")
-- MAGIC     .mode("overwrite")
-- MAGIC     .saveAsTable("restaurant_lakehouse.silver.fact_reviews")
-- MAGIC )

-- COMMAND ----------


SELECT
    r.name AS restaurant_name,
    COUNT(o.order_id) AS total_orders,
    ROUND(SUM(o.total_amount), 2) AS total_revenue,
    ROUND(AVG(o.total_amount), 2) AS average_order_value
FROM restaurant_lakehouse.silver.fact_orders o
JOIN restaurant_lakehouse.silver.dim_restaurants r
    ON o.restaurant_id = r.restaurant_id
GROUP BY r.restaurant_id, r.name
ORDER BY total_revenue DESC;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql import functions as F
-- MAGIC from pyspark.sql.types import *
-- MAGIC
-- MAGIC orders = spark.table(
-- MAGIC     "restaurant_lakehouse.bronze.historical_orders"
-- MAGIC )
-- MAGIC
-- MAGIC print("Orders loaded:", orders.count())

-- COMMAND ----------

-- MAGIC %python
-- MAGIC sample_json = (
-- MAGIC     orders
-- MAGIC     .select("items")
-- MAGIC     .filter(
-- MAGIC         F.col("items").isNotNull() &
-- MAGIC         (F.trim(F.col("items")) != "")
-- MAGIC     )
-- MAGIC     .first()["items"]
-- MAGIC )
-- MAGIC
-- MAGIC print(sample_json)
-- MAGIC item_schema = spark.range(1).select(
-- MAGIC     F.schema_of_json(F.lit(sample_json)).alias("schema")
-- MAGIC ).first()["schema"]
-- MAGIC
-- MAGIC print(item_schema)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC fact_orders_base = spark.table(
-- MAGIC     "restaurant_lakehouse.silver.fact_orders"
-- MAGIC )
-- MAGIC
-- MAGIC orders_with_items = (
-- MAGIC     fact_orders_base
-- MAGIC     .join(
-- MAGIC         orders.select("order_id", "items"),
-- MAGIC         on="order_id",
-- MAGIC         how="left"
-- MAGIC     )
-- MAGIC     .withColumn(
-- MAGIC         "parsed_items",
-- MAGIC         F.from_json(
-- MAGIC             F.col("items"),
-- MAGIC             item_schema
-- MAGIC         )
-- MAGIC     )
-- MAGIC )

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql import functions as F
-- MAGIC
-- MAGIC orders = spark.table(
-- MAGIC     "restaurant_lakehouse.bronze.historical_orders"
-- MAGIC )
-- MAGIC
-- MAGIC orders.select(
-- MAGIC     "order_id",
-- MAGIC     "items"
-- MAGIC ).show(10, truncate=False)
-- MAGIC row = (
-- MAGIC     orders
-- MAGIC     .filter(F.col("items").isNotNull())
-- MAGIC     .select("items")
-- MAGIC     .first()
-- MAGIC )
-- MAGIC
-- MAGIC print(repr(row["items"]))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC orders_clean = (
-- MAGIC     orders
-- MAGIC     .filter(F.col("order_id").isNotNull())
-- MAGIC     .withColumn(
-- MAGIC         "total_amount_clean",
-- MAGIC         F.regexp_replace(
-- MAGIC             F.col("total_amount"),
-- MAGIC             r"[^0-9.-]",
-- MAGIC             ""
-- MAGIC         )
-- MAGIC     )
-- MAGIC     .withColumn(
-- MAGIC         "total_amount",
-- MAGIC         F.expr(
-- MAGIC             "try_cast(total_amount_clean AS DOUBLE)"
-- MAGIC         )
-- MAGIC     )
-- MAGIC     .drop("total_amount_clean")
-- MAGIC     .withColumn(
-- MAGIC         "created_at",
-- MAGIC         F.expr(
-- MAGIC             "try_cast(created_at AS TIMESTAMP)"
-- MAGIC         )
-- MAGIC     )
-- MAGIC     .withColumn(
-- MAGIC         "order_type",
-- MAGIC         F.trim(F.col("order_type"))
-- MAGIC     )
-- MAGIC     .withColumn(
-- MAGIC         "payment_method",
-- MAGIC         F.trim(F.col("payment_method"))
-- MAGIC     )
-- MAGIC     .withColumn(
-- MAGIC         "order_status",
-- MAGIC         F.trim(F.col("order_status"))
-- MAGIC     )
-- MAGIC     .dropDuplicates(["order_id"])
-- MAGIC )

-- COMMAND ----------

-- MAGIC %python
-- MAGIC fact_orders = orders_clean.select(
-- MAGIC     "order_id",
-- MAGIC     "timestamp",
-- MAGIC     "restaurant_id",
-- MAGIC     "customer_id",
-- MAGIC     "order_type",
-- MAGIC     "total_amount",
-- MAGIC     "payment_method",
-- MAGIC     "order_status",
-- MAGIC     "created_at"
-- MAGIC )
-- MAGIC
-- MAGIC (
-- MAGIC     fact_orders.write
-- MAGIC     .format("delta")
-- MAGIC     .mode("overwrite")
-- MAGIC     .option("overwriteSchema", "true")
-- MAGIC     .saveAsTable(
-- MAGIC         "restaurant_lakehouse.silver.fact_orders"
-- MAGIC     )
-- MAGIC )
-- MAGIC schema_string = (
-- MAGIC     spark
-- MAGIC     .range(1)
-- MAGIC     .select(
-- MAGIC         F.schema_of_json(
-- MAGIC             F.lit(sample_json)
-- MAGIC         ).alias("schema")
-- MAGIC     )
-- MAGIC     .first()["schema"]
-- MAGIC )
-- MAGIC
-- MAGIC print(schema_string)
-- MAGIC sample_json = (
-- MAGIC     orders
-- MAGIC     .filter(
-- MAGIC         F.col("items").isNotNull() &
-- MAGIC         (F.trim(F.col("items")) != "")
-- MAGIC     )
-- MAGIC     .select("items")
-- MAGIC     .first()["items"]
-- MAGIC )
-- MAGIC
-- MAGIC print(sample_json)
-- MAGIC schema_string = (
-- MAGIC     spark
-- MAGIC     .range(1)
-- MAGIC     .select(
-- MAGIC         F.schema_of_json(
-- MAGIC             F.lit(sample_json)
-- MAGIC         ).alias("schema")
-- MAGIC     )
-- MAGIC     .first()["schema"]
-- MAGIC )
-- MAGIC
-- MAGIC print(schema_string)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC orders_with_items = (
-- MAGIC     orders
-- MAGIC     .withColumn(
-- MAGIC         "parsed_items",
-- MAGIC         F.from_json(
-- MAGIC             F.col("items"),
-- MAGIC             schema_string
-- MAGIC         )
-- MAGIC     )
-- MAGIC )
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC orders = spark.table(
-- MAGIC     "restaurant_lakehouse.bronze.historical_orders"
-- MAGIC )
-- MAGIC
-- MAGIC raw_item = (
-- MAGIC     orders
-- MAGIC     .filter(F.col("items").isNotNull())
-- MAGIC     .select("items")
-- MAGIC     .first()["items"]
-- MAGIC )
-- MAGIC
-- MAGIC print("NORMAL:")
-- MAGIC print(raw_item)
-- MAGIC
-- MAGIC print("\nREPR:")
-- MAGIC print(repr(raw_item))
-- MAGIC
-- MAGIC print("\nLENGTH:")
-- MAGIC print(len(raw_item))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC raw_csv = spark.read.text(
-- MAGIC     "/Volumes/restaurant_lakehouse/landing/restaurant_files/historical_orders.csv"
-- MAGIC )
-- MAGIC
-- MAGIC raw_csv.show(3, truncate=False)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql import functions as F
-- MAGIC from pyspark.sql.types import *

-- COMMAND ----------

-- MAGIC %python
-- MAGIC fixed_orders = (
-- MAGIC     spark.read
-- MAGIC     .option("header", "true")
-- MAGIC     .option("inferSchema", "true")
-- MAGIC     .option("quote", '"')
-- MAGIC     .option("escape", '"')
-- MAGIC     .option("multiLine", "true")
-- MAGIC     .option("mode", "PERMISSIVE")
-- MAGIC     .csv(
-- MAGIC         "/Volumes/restaurant_lakehouse/landing/restaurant_files/historical_orders.csv"
-- MAGIC     )
-- MAGIC )

-- COMMAND ----------

-- MAGIC %python
-- MAGIC fixed_orders.select(
-- MAGIC     "order_id",
-- MAGIC     "items",
-- MAGIC     "total_amount",
-- MAGIC     "payment_method",
-- MAGIC     "order_status",
-- MAGIC     "created_at"
-- MAGIC ).show(5, truncate=False)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC (
-- MAGIC     fixed_orders.write
-- MAGIC     .format("delta")
-- MAGIC     .mode("overwrite")
-- MAGIC     .option("overwriteSchema", "true")
-- MAGIC     .saveAsTable(
-- MAGIC         "restaurant_lakehouse.bronze.historical_orders"
-- MAGIC     )
-- MAGIC )

-- COMMAND ----------

-- MAGIC %python
-- MAGIC orders = spark.table(
-- MAGIC     "restaurant_lakehouse.bronze.historical_orders"
-- MAGIC )

-- COMMAND ----------

-- MAGIC %python
-- MAGIC orders.printSchema()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC orders_clean = (
-- MAGIC     orders
-- MAGIC     .filter(F.col("order_id").isNotNull())
-- MAGIC     .withColumn(
-- MAGIC         "total_amount",
-- MAGIC         F.expr("try_cast(total_amount AS DOUBLE)")
-- MAGIC     )
-- MAGIC     .withColumn(
-- MAGIC         "created_at",
-- MAGIC         F.expr("try_cast(created_at AS TIMESTAMP)")
-- MAGIC     )
-- MAGIC     .withColumn(
-- MAGIC         "timestamp",
-- MAGIC         F.expr("try_cast(timestamp AS TIMESTAMP)")
-- MAGIC     )
-- MAGIC     .withColumn(
-- MAGIC         "order_type",
-- MAGIC         F.trim("order_type")
-- MAGIC     )
-- MAGIC     .withColumn(
-- MAGIC         "payment_method",
-- MAGIC         F.trim("payment_method")
-- MAGIC     )
-- MAGIC     .withColumn(
-- MAGIC         "order_status",
-- MAGIC         F.trim("order_status")
-- MAGIC     )
-- MAGIC     .dropDuplicates(["order_id"])
-- MAGIC )

-- COMMAND ----------

-- MAGIC %python
-- MAGIC fact_orders = orders_clean.select(
-- MAGIC     "order_id",
-- MAGIC     "timestamp",
-- MAGIC     "restaurant_id",
-- MAGIC     "customer_id",
-- MAGIC     "order_type",
-- MAGIC     "total_amount",
-- MAGIC     "payment_method",
-- MAGIC     "order_status",
-- MAGIC     "created_at"
-- MAGIC )

-- COMMAND ----------

-- MAGIC %python
-- MAGIC (
-- MAGIC     fact_orders.write
-- MAGIC     .format("delta")
-- MAGIC     .mode("overwrite")
-- MAGIC     .option("overwriteSchema", "true")
-- MAGIC     .saveAsTable(
-- MAGIC         "restaurant_lakehouse.silver.fact_orders"
-- MAGIC     )
-- MAGIC )

-- COMMAND ----------

-- MAGIC %python
-- MAGIC orders_with_items = (
-- MAGIC     orders
-- MAGIC     .withColumn(
-- MAGIC         "parsed_items",
-- MAGIC         F.from_json(
-- MAGIC             F.col("items"),
-- MAGIC             item_schema
-- MAGIC         )
-- MAGIC     )
-- MAGIC )

-- COMMAND ----------

-- MAGIC %python
-- MAGIC orders.select("items").show(2, truncate=False)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC item_schema_ddl = """
-- MAGIC ARRAY<STRUCT<
-- MAGIC     item_id: STRING,
-- MAGIC     name: STRING,
-- MAGIC     category: STRING,
-- MAGIC     quantity: INT,
-- MAGIC     unit_price: DOUBLE,
-- MAGIC     subtotal: DOUBLE
-- MAGIC >>
-- MAGIC """
-- MAGIC
-- MAGIC print(item_schema_ddl)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC orders_with_items = (
-- MAGIC     orders
-- MAGIC     .withColumn(
-- MAGIC         "parsed_items",
-- MAGIC         F.from_json(
-- MAGIC             F.col("items"),
-- MAGIC             item_schema_ddl
-- MAGIC         )
-- MAGIC     )
-- MAGIC )

-- COMMAND ----------

-- MAGIC %python
-- MAGIC orders_with_items.printSchema()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(
-- MAGIC     orders_with_items.select(
-- MAGIC         "order_id",
-- MAGIC         "parsed_items"
-- MAGIC     ).limit(10)
-- MAGIC )

-- COMMAND ----------

-- MAGIC %python
-- MAGIC exploded_items = (
-- MAGIC     orders_with_items
-- MAGIC     .select(
-- MAGIC         "order_id",
-- MAGIC         "restaurant_id",
-- MAGIC         F.explode("parsed_items").alias("item")
-- MAGIC     )
-- MAGIC )

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(
-- MAGIC     exploded_items.select(
-- MAGIC         "order_id",
-- MAGIC         "restaurant_id",
-- MAGIC         "item.*"
-- MAGIC     ).limit(20)
-- MAGIC )

-- COMMAND ----------

-- MAGIC %python
-- MAGIC fact_order_items = (
-- MAGIC     exploded_items
-- MAGIC     .select(
-- MAGIC         "order_id",
-- MAGIC         "restaurant_id",
-- MAGIC         F.col("item.item_id").alias("item_id"),
-- MAGIC         F.col("item.name").alias("item_name"),
-- MAGIC         F.col("item.category").alias("category"),
-- MAGIC         F.col("item.quantity").alias("quantity"),
-- MAGIC         F.col("item.unit_price").alias("unit_price"),
-- MAGIC         F.col("item.subtotal").alias("subtotal")
-- MAGIC     )
-- MAGIC )

-- COMMAND ----------

-- MAGIC %python
-- MAGIC (
-- MAGIC     fact_order_items.write
-- MAGIC     .format("delta")
-- MAGIC     .mode("overwrite")
-- MAGIC     .option("overwriteSchema", "true")
-- MAGIC     .saveAsTable(
-- MAGIC         "restaurant_lakehouse.silver.fact_order_items"
-- MAGIC     )
-- MAGIC )

-- COMMAND ----------

-- MAGIC %python
-- MAGIC item_schema_ddl = "ARRAY<STRUCT<item_id:STRING,name:STRING,category:STRING,quantity:INT,unit_price:DOUBLE,subtotal:DOUBLE>>"

-- COMMAND ----------


SELECT
    item_name,
    category,
    SUM(quantity) AS units_sold,
    ROUND(SUM(subtotal), 2) AS revenue
FROM restaurant_lakehouse.silver.fact_order_items
GROUP BY
    item_name,
    category
ORDER BY revenue DESC
LIMIT 20;

-- COMMAND ----------

