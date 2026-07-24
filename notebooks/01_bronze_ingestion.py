# Databricks notebook source
# MAGIC %sql
# MAGIC
# MAGIC SELECT COUNT(*) AS customer_count
# MAGIC FROM restaurant_lakehouse.bronze.customers;

# COMMAND ----------

restaurants_df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(
        "/Volumes/restaurant_lakehouse/landing/restaurant_files/restaurants.csv"
    )
)

display(restaurants_df)

# COMMAND ----------

restaurants_df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(
        "/Volumes/restaurant_lakehouse/landing/restaurant_files/restaurants.csv"
    )
)

display(restaurants_df)
(
    restaurants_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(
        "restaurant_lakehouse.bronze.restaurants"
    )
)
menu_items_df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(
        "/Volumes/restaurant_lakehouse/landing/restaurant_files/menu_items.csv"
    )
)

display(menu_items_df)

# COMMAND ----------

(
    menu_items_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(
        "restaurant_lakehouse.bronze.menu_items"
    )
)

# COMMAND ----------

display(
    dbutils.fs.ls(
        "/Volumes/restaurant_lakehouse/landing/restaurant_files/"
    )
)

# COMMAND ----------

customer_reviews_df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(
        "/Volumes/restaurant_lakehouse/landing/restaurant_files/customer_reviews.csv"
    )
)

display(customer_reviews_df)
(
    customer_reviews_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(
        "restaurant_lakehouse.bronze.reviews"
    )
)

# COMMAND ----------

tables = [
    "customers",
    "restaurants",
    "menu_items",
    "historical_orders",
    "reviews"
]

for table in tables:
    print("\n" + "=" * 60)
    print(f"TABLE: {table}")
    print("=" * 60)

    df = spark.table(f"restaurant_lakehouse.bronze.{table}")
    df.printSchema()

# COMMAND ----------

historical_orders_df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .option("quote", '"')
    .option("escape", '"')
    .option("multiLine", "true")
    .option("mode", "PERMISSIVE")
    .csv(
        "/Volumes/restaurant_lakehouse/landing/restaurant_files/historical_orders.csv"
    )
)

# COMMAND ----------

(
    historical_orders_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(
        "restaurant_lakehouse.bronze.historical_orders"
    )
)

# COMMAND ----------

base_path = "/Volumes/restaurant_lakehouse/landing/restaurant_files/"

# COMMAND ----------

customers_path = base_path + "customers.csv"
restaurants_path = base_path + "restaurants.csv"
menu_items_path = base_path + "menu_items.csv"
orders_path = base_path + "historical_orders.csv"
reviews_path = base_path + "customer_reviews.csv"

# COMMAND ----------

orders_check = spark.table(
    "restaurant_lakehouse.bronze.historical_orders"
)

orders_check.select(
    "order_id",
    "items",
    "total_amount"
).show(2, truncate=False)

# COMMAND ----------

historical_orders_df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .option("quote", '"')
    .option("escape", '"')
    .option("multiLine", "true")
    .option("mode", "PERMISSIVE")
    .csv(
        "/Volumes/restaurant_lakehouse/landing/restaurant_files/historical_orders.csv"
    )
)

# COMMAND ----------

print("Historical orders:", historical_orders_df.count())

historical_orders_df.printSchema()

# COMMAND ----------

(
    historical_orders_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(
        "restaurant_lakehouse.bronze.historical_orders"
    )
)

# COMMAND ----------

print("bronze.historical_orders created successfully")

# COMMAND ----------

