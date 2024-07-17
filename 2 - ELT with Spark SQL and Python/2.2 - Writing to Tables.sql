-- Databricks notebook source
-- MAGIC %run ../Includes/Copy-Datasets

-- COMMAND ----------

USE imfilip;
SHOW TABLES;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC files = dbutils.fs.ls("dbfs:/mnt/demo-datasets/bookstore")
-- MAGIC display(files)

-- COMMAND ----------

CREATE OR REPLACE TABLE orders AS
  SELECT * FROM parquet.`dbfs:/mnt/demo-datasets/bookstore/orders/`;

-- COMMAND ----------

SELECT * FROM orders;

-- COMMAND ----------

DESCRIBE HISTORY orders;

-- COMMAND ----------

-- w porównaniu do 'create or replace':
-- poniższa metoda zadziała na już istniejących tabelach, nie stworzy nowej
-- nadpisuje tylko te rekordy, które pasują wg schematu danych (schema)

INSERT OVERWRITE orders
SELECT * FROM PARQUET.`dbfs:/mnt/demo-datasets/bookstore/orders/`;

-- COMMAND ----------

DESCRIBE HISTORY orders;

-- COMMAND ----------

INSERT OVERWRITE orders
SELECT *, current_timestamp() FROM PARQUET.`dbfs:/mnt/demo-datasets/bookstore/orders/`;

-- COMMAND ----------

INSERT INTO orders
SELECT * FROM PARQUET.`dbfs:/mnt/demo-datasets/bookstore/orders-new/`;

-- COMMAND ----------

SELECT count(*) FROM orders;

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW customers_updates AS
SELECT * FROM JSON.`dbfs:/mnt/demo-datasets/bookstore/customers-json-new/`;

MERGE INTO customers c
USING customers_updates u
ON c.customer_id = u.customer_id
WHEN MATCHED AND c.email IS NULL AND u.email IS NOT NULL THEN
  UPDATE SET email = u.email, updated = u.updated
WHEN NOT MATCHED THEN INSERT *;

-- COMMAND ----------

SELECT * FROM customers;

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW books_update
  (book_id STRING, title STRING, author STRING, category STRING, price DOUBLE)
USING CSV
OPTIONS (
  path="dbfs:/mnt/demo-datasets/bookstore/books-csv-new/",
  header="true",
  delimiter=";"
)

-- COMMAND ----------

SELECT * FROM books_update;

-- COMMAND ----------

MERGE INTO books b
USING books_update u
ON b.book_id = u.book_id AND b.title = u.title
WHEN NOT MATCHED AND u.category = 'Computer Science' THEN INSERT *;

-- COMMAND ----------

MERGE INTO books b
USING books_update u
ON b.book_id = u.book_id AND b.title = u.title
WHEN NOT MATCHED AND u.category <> 'Computer Science' THEN INSERT *;

-- COMMAND ----------

SELECT * FROM books_update;

-- COMMAND ----------


