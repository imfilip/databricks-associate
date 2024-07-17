-- Databricks notebook source
USE imfilip;
SELECT * FROM employee;

-- COMMAND ----------

DESCRIBE HISTORY employee;

-- COMMAND ----------

SELECT * 
FROM employee VERSION AS OF 3;

-- COMMAND ----------

SELECT * 
FROM employee@v3;

-- COMMAND ----------

DELETE FROM employee;

-- COMMAND ----------

SELECT * FROM employee;

-- COMMAND ----------

DESCRIBE HISTORY employee;

-- COMMAND ----------

RESTORE TABLE employee TO VERSION AS OF 5;

-- COMMAND ----------

DESCRIBE HISTORY employee;

-- COMMAND ----------

DESCRIBE DETAIL employee;

-- COMMAND ----------

UPDATE employee
SET salary = salary * 2
WHERE name LIKE "%o%";

-- COMMAND ----------

DESCRIBE EXTENDED employee;

-- COMMAND ----------

DESCRIBE DETAIL employee;

-- COMMAND ----------

DESCRIBE HISTORY employee;

-- COMMAND ----------

OPTIMIZE employee
ZORDER BY id;

-- COMMAND ----------

DESCRIBE DETAIL employee;

-- COMMAND ----------

DESCRIBE HISTORY employee;

-- COMMAND ----------

DELETE FROM employee
WHERE name LIKE "A%";

-- COMMAND ----------

DESCRIBE DETAIL employee;

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/user/hive/warehouse/imfilip.db/employee

-- COMMAND ----------

INSERT INTO employee
VALUES
  (7, "Marcin", 21000.0);

-- COMMAND ----------

DESCRIBE DETAIL employee;

-- COMMAND ----------

DESCRIBE HISTORY employee;

-- COMMAND ----------

OPTIMIZE employee
ZORDER BY (id);

-- COMMAND ----------

DESCRIBE HISTORY employee;

-- COMMAND ----------

DESCRIBE DETAIL employee;

-- COMMAND ----------

VACUUM employee;

-- COMMAND ----------

DESCRIBE HISTORY employee;

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/user/hive/warehouse/imfilip.db/employee

-- COMMAND ----------

-- do not do that on production
SET spark.databricks.delta.retentionDurationCheck.enabled = false;

-- COMMAND ----------

VACUUM employee RETAIN 0 HOURS;

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/user/hive/warehouse/imfilip.db/employee

-- COMMAND ----------

DESCRIBE HISTORY employee;

-- COMMAND ----------

DROP TABLE employee;

-- COMMAND ----------

SHOW TABLES;

-- COMMAND ----------


