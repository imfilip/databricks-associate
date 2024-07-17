# Databricks notebook source
# MAGIC %sql
# MAGIC SHOW DATABASES;

# COMMAND ----------

list_of_db = spark.sql("SHOW DATABASES").rdd.map(lambda x: x[0]).collect()
for db in list_of_db:
    if db in ["default", "_default_location"]:
        continue
    spark.sql(f"DROP SCHEMA {db} CASCADE")

spark.sql("SHOW SCHEMAS").show()

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS imfilip;

# COMMAND ----------

# MAGIC %sql
# MAGIC USE imfilip;
# MAGIC SHOW TABLES;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE smartphones
# MAGIC (id INT, name STRING, brand STRING, year INT);
# MAGIC
# MAGIC
# MAGIC
# MAGIC INSERT INTO smartphones
# MAGIC VALUES (1, 'iPhone 14', 'Apple', 2022),
# MAGIC (2, 'iPhone 13', 'Apple', 2021),
# MAGIC (3, 'iPhone 6', 'Apple', 2014),
# MAGIC (4, 'iPad Air', 'Apple', 2013),
# MAGIC (5, 'Galaxy S22', 'Samsung', 2022),
# MAGIC (6, 'Galaxy Z Fold', 'Samsung', 2022),
# MAGIC (7, 'Galaxy S9', 'Samsung', 2016),
# MAGIC (8, '12 Pro', 'Xiaomi', 2022),
# MAGIC (9, 'Redmi 11T Pro', 'Xiaomi', 2022),
# MAGIC (10, 'Redmi Note 11', 'Xiaomi', 2021)

# COMMAND ----------

# MAGIC %sql 
# MAGIC DESCRIBE smartphones;
# MAGIC -- DESCRIBE EXTENDED smartphones;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM smartphones;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE VIEW view_apple_smartphones AS
# MAGIC SELECT * FROM smartphones
# MAGIC WHERE brand = 'Apple';

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM view_apple_smartphones;

# COMMAND ----------

spark.read.table('view_apple_smartphones').display(100)

# COMMAND ----------

spark.sql(
    """
    CREATE TEMP VIEW temp_view_phones_brand AS
    SELECT DISTINCT brand FROM smartphones;
    """
)

# COMMAND ----------

spark.read.table('temp_view_phones_brand').display()

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES;

# COMMAND ----------

spark.sql(
    """
    CREATE GLOBAL TEMP VIEW global_temp_view_latest_phones AS
    SELECT * FROM smartphones
    WHERE year > 2020
    ORDER BY year DESC
    """
)

# COMMAND ----------

spark.sql("SHOW TABLES").display()

# COMMAND ----------

spark.sql("SHOW TABLES FROM global_temp").display()
spark.sql("SHOW TABLES IN global_temp").display()

# COMMAND ----------


