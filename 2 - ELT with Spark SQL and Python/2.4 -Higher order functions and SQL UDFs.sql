-- Databricks notebook source
-- MAGIC %run ../Includes/Copy-Datasets

-- COMMAND ----------

USE imfilip;
SELECT * FROM orders;

-- COMMAND ----------

SELECT * FROM
(
  SELECT 
    order_id,
    books,
    FILTER(books, i -> i.quantity >= 2) AS multiple_copies
  FROM orders
) o
WHERE size(o.multiple_copies) > 0;

-- COMMAND ----------

SELECT 
  order_id,
  books,
  TRANSFORM(
    books,
    b -> CAST(b.subtotal * 0.8 AS INT)
  ) AS subtotal_after_discount,
  aggregate(
    TRANSFORM(
      books,
      b -> CAST(b.subtotal * 0.8 AS INT)
      ), 0, (x, y) -> x + y
    ) AS sliced
FROM orders;

-- COMMAND ----------

CREATE OR REPLACE FUNCTION get_url(par STRING)
RETURNS STRING
RETURN concat("https://www.", split(par, "@")[1])

-- COMMAND ----------

SELECT email, get_url(email) domain
FROM customers;

-- COMMAND ----------

CREATE OR REPLACE FUNCTION site_type(par STRING)
RETURNS STRING
RETURN CASE
          WHEN par LIKE "%.com" THEN "Commercial business"
          WHEN par LIKE "%.org" THEN "Non-profits organization"
          WHEN par LIKE "%.edu" THEN "Educational institution"
          ELSE concat("Unknown extension for domain: ", split(par, "@")[1])
        END

-- COMMAND ----------

SELECT email, get_url(email), site_type(email)
FROM customers;

-- COMMAND ----------

DROP FUNCTION get_url;
DROP FUNCTION site_type;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import udf
-- MAGIC from pyspark.sql.types import StringType
-- MAGIC
-- MAGIC @udf(returnType=StringType())
-- MAGIC def get_url_python(text):
-- MAGIC     return text.split("@")[1]

-- COMMAND ----------

DESCRIBE FUNCTION EXTENDED get_url_python;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC _ = spark.udf.register("get_url_python", get_url_python)

-- COMMAND ----------

DESCRIBE FUNCTION EXTENDED get_url_python;

-- COMMAND ----------


