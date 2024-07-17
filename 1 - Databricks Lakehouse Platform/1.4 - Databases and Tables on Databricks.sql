-- Databricks notebook source
USE default;

-- COMMAND ----------

CREATE TABLE managed_default
  (width INT, length INT, height INT);

INSERT INTO managed_default
VALUES
  (3 INT, 2.0 INT, 1 INT);

SELECT * FROM managed_default;

-- COMMAND ----------

DESCRIBE EXTENDED managed_default;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.mkdirs("dbfs:/mnt/demo")

-- COMMAND ----------

-- %python
-- for i in [ob.name[:-1] for ob in dbutils.fs.ls('dbfs:/user/hive/warehouse') if not 'db' in ob.name]:
--   spark.sql(f"DROP TABLE {i}")

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/mnt/

-- COMMAND ----------

CREATE TABLE external_default
  (id INT, name STRING, age INT)
LOCATION 'dbfs:/mnt/demo';


-- COMMAND ----------

INSERT INTO external_default
VALUES
  (1, "Marcin", 33)

-- COMMAND ----------

DESCRIBE EXTENDED external_default;

-- COMMAND ----------

DROP TABLE external_default;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.rm("dbfs:/mnt/demo", True)

-- COMMAND ----------

CREATE SCHEMA imfilip2
LOCATION "dbfs:/mnt/demo/imfilip2.db"

-- COMMAND ----------

DESCRIBE SCHEMA EXTENDED imfilip2 

-- COMMAND ----------


