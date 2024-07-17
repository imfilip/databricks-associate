# Databricks notebook source
# MAGIC %sql
# MAGIC SHOW DATABASES;

# COMMAND ----------

# MAGIC %sql
# MAGIC USE imfilip;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN global_temp;

# COMMAND ----------

spark.sql(
    """
    SELECT * FROM global_temp.global_temp_view_latest_phones;
    """
).display()

# COMMAND ----------


