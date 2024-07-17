# Databricks notebook source
# MAGIC %run ../Includes/Copy-Datasets

# COMMAND ----------

(
    spark
    .readStream
    .table('books')
    .createOrReplaceTempView("books_streaming_tmp_vw")
)

# COMMAND ----------

# MAGIC %md
# MAGIC
