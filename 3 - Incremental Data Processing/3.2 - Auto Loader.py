# Databricks notebook source
# MAGIC %run ../Includes/Copy-Datasets

# COMMAND ----------

files = dbutils.fs.ls(f"{dataset_bookstore}/orders-raw")
display(files)

# COMMAND ----------

(
    spark
        .readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "parquet")
            .option("cloudFiles.schemaLocation", "dbfs:/mnt/demo/orders_checkpoint")
            .load(f"{dataset_bookstore}/orders-raw")
        .writeStream
            .option("checkpointLocation", "dbfs:/mnt/demo/orders_checkpoint")
            .table("orders_update")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM orders_update;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM orders_update;

# COMMAND ----------

load_new_data()

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY orders_update;

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE orders_update

# COMMAND ----------

dbutils.fs.rm("dbfs:/mnt/demo/orders_checkpoint", True)

# COMMAND ----------


