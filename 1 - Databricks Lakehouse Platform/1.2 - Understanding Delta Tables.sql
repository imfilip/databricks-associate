-- Databricks notebook source
USE imfilip;
CREATE OR REPLACE TABLE employee
-- USING DELTA - it is by default
  (id INT, name STRING, salary DOUBLE);

-- COMMAND ----------

INSERT INTO employee
VALUES
  (1, "Adam", 3500.0),
  (2, "Sarah", 4020.5),
  (3, "John", 2999.3),
  (4, "Thomas", 4000.3),
  (5, "Anna", 2500.0),
  (6, "Kim", 6200.3) 

-- COMMAND ----------

DESCRIBE HISTORY employee;

-- COMMAND ----------

SELECT * FROM employee;

-- COMMAND ----------

DESCRIBE DETAIL employee;

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/user/hive/warehouse/imfilip.db/employee

-- COMMAND ----------

UPDATE employee
SET salary = salary + 10
WHERE name LIKE "A%";

-- COMMAND ----------

SELECT * FROM employee;

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/user/hive/warehouse/imfilip.db/employee

-- COMMAND ----------

DESCRIBE DETAIL employee;

-- COMMAND ----------

DESCRIBE HISTORY employee;

-- COMMAND ----------

DESCRIBE EXTENDED employee;

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/user/hive/warehouse/imfilip.db/employee/_delta_log

-- COMMAND ----------

-- MAGIC %fs head dbfs:/user/hive/warehouse/imfilip.db/employee/_delta_log/00000000000000000000.json

-- COMMAND ----------


