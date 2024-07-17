-- Databricks notebook source
USE imfilip;
CREATE TABLE family
  (id INT, name STRING, age DOUBLE);

INSERT INTO family
VALUES
  (1, "Marcin", 32.7),
  (2, "Ewelina", 29.1),
  (3, "Mikolaj", 1.45),
  (4, "Hanna", 0.1);

-- COMMAND ----------

SELECT * FROM family;

-- COMMAND ----------

CREATE TABLE family2
COMMENT "Contain info of Filip's family"
PARTITIONED BY (id, name)
LOCATION "dbfs:/user/mnt/family2"
AS
SELECT id, name, ROUND(age) as age FROM family;

-- COMMAND ----------

DESCRIBE EXTENDED family2;

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/user/mnt/family2

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/user/mnt/family2/id=1/

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/user/mnt/family2/id=1/name=Marcin/

-- COMMAND ----------

ALTER TABLE family ADD CONSTRAINT age_validation CHECK (age > 0);

-- COMMAND ----------

ALTER TABLE family ADD CONSTRAINT name_validation CHECK (name NOT LIKE 'Mar%');

-- COMMAND ----------

ALTER TABLE family ALTER COLUMN name_null_validation SET NOT NULL;

-- COMMAND ----------

DESCRIBE family2;

-- COMMAND ----------

CREATE TABLE family_deep
DEEP CLONE family;

-- COMMAND ----------

SELECT * FROM family_deep;

-- COMMAND ----------

CREATE TABLE family_shallow
SHALLOW CLONE family;

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/user/hive/warehouse/imfilip.db/family_shallow

-- COMMAND ----------


