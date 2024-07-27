# Databricks notebook source
path = "dbfs:/user/mnt/csv_files"
schema = spark.read.csv(f"{path}/0.csv", header=True, sep=";").schema
df = (
    spark
        .readStream
            .format("csv")
            .option("header", True)
            .option("sep", ";")
            .schema(schema)
            .load(f"{path}")
)

# COMMAND ----------

df.display()

# COMMAND ----------

df2 = (
    spark
        .readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("header", True)
            .option("sep", ";")
            .schema(schema)
            .load(f"{path}")
  )

# COMMAND ----------

df2.display()

# COMMAND ----------


