# Databricks notebook source
# MAGIC %run ../Includes/Copy-Datasets

# COMMAND ----------

files = dbutils.fs.ls(f"{dataset_bookstore}/orders-raw")
display(files)

# COMMAND ----------

import pyspark.sql.functions as F

(
    spark
        .readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "parquet")
            .option("cloudFiles.schemaLocation", "dbfs:/mnt/demo/checkpoints/orders_raw")
            .load(f"{dataset_bookstore}/orders-raw")
            .select("*", F.current_timestamp().alias("arrival_time"), F.input_file_name().alias("source_file"))
        .writeStream
            .format("delta")
            .option("checkpointLocation", "dbfs:/mnt/demo/checkpoints/orders_bronze")
            .outputMode("append")
            .table("orders_bronze")
)

# COMMAND ----------

spark.sql("""SELECT count(*) FROM orders_bronze;""").display()

# COMMAND ----------

load_new_data()

# COMMAND ----------



# COMMAND ----------

### How to ingest JSON file so that profile column will be StructType instead of StringType?

(
    spark
        .read
        .format("json")
        .option("multiLine", True)
        .option("header",True)
        .option("inferschema",True)
        .load(f"{dataset_bookstore}/customers-json")
        .printSchema()
)

# COMMAND ----------

spark.sql("""SELECT * FROM customers_lookup;""").display()

# COMMAND ----------

customers_lookup = spark.table("customers_lookup")

(
    spark
        .readStream
        .table("orders_bronze")
        .join(customers_lookup, on=["customer_id"], how="inner")
        .select("order_id", "quantity", "customer_id", "profile:first_name")
        .display()
)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW orders_enriched_tmp AS (
# MAGIC   SELECT order_id, quantity, o.customer_id, c.profile:first_name as f_name, c.profile:last_name as l_name,
# MAGIC     CAST(from_unixtime(order_timestamp, 'yyyy-MM-dd HH:mm:ss') AS TIMESTAMP) order_timestamp, books
# MAGIC   FROM orders_bronze_tmp o
# MAGIC   INNER JOIN customers_lookup c ON o.customer_id = c.customer_id
# MAGIC   WHERE quantity > 0 
# MAGIC )

# COMMAND ----------

(
    spark
        .table("orders_enriched_tmp")
        .writeStream
        .format("delta")
        .option("checkpointLocation", "dbfs:/mnt/demo/checkpoints/order_silver")
        .outputMode("append")
        .table("orders_silver")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM orders_silver;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM orders_silver;

# COMMAND ----------

load_new_data()

# COMMAND ----------

(
    spark
        .readStream
        .table("orders_silver")
        .createOrReplaceTempView("orders_silver_tmp")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW daily_customer_books_tmp AS (
# MAGIC   SELECT customer_id, f_name, l_name, date_trunc("DD", order_timestamp) order_date, sum(quantity) books_counts
# MAGIC   FROM orders_silver_tmp
# MAGIC   GROUP BY customer_id, f_name, l_name, date_trunc("DD", order_timestamp)
# MAGIC )

# COMMAND ----------

(
    spark
        .table("daily_customer_books_tmp")
        .writeStream
        .format("delta")
        .outputMode("complete")
        .option("checkpointLocation", "dbfs:/mnt/demo/checkpoints/daily_customer_books")
        .trigger(availableNow=True)
        .table("daily_customer_books")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM daily_customer_books;

# COMMAND ----------

load_new_data(all=True)

# COMMAND ----------

for s in spark.streams.active:
    print(f"Stopping stream {s.id}")
    s.stop()
    s.awaitTermination()

# COMMAND ----------

for s in spark.streams.active:
    print(f"Active stream {s.id}")

# COMMAND ----------


