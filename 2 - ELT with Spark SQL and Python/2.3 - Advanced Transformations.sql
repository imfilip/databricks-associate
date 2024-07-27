-- Databricks notebook source
-- MAGIC %run ../Includes/Copy-Datasets

-- COMMAND ----------

USE imfilip;
SELECT * FROM books;

-- COMMAND ----------

select * from customers;

-- COMMAND ----------

DESCRIBE customers;

-- COMMAND ----------

select customer_id, profile:first_name, profile:address:country
from customers;

-- COMMAND ----------

SELECT from_json(profile) as profile_struct FROM customers;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC schema_profile = spark.sql("SELECT profile FROM customers LIMIT 1;").collect()[0]["profile"]
-- MAGIC schema_profile

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC spark.sql(
-- MAGIC     f"""
-- MAGIC     CREATE OR REPLACE TEMP VIEW parsed_customers AS
-- MAGIC       SELECT customer_id, from_json(profile, schema_of_json('{schema_profile}')) AS profile_struct
-- MAGIC     FROM customers
-- MAGIC     """
-- MAGIC )
-- MAGIC
-- MAGIC spark.sql("SELECT * FROM parsed_customers;").display()

-- COMMAND ----------

DESCRIBE parsed_customers;

-- COMMAND ----------

SELECT customer_id, profile_struct.first_name, profile_struct.address.country
FROM parsed_customers;

-- COMMAND ----------



-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW customers_final AS
  SELECT customer_id, profile_struct.*, profile_struct.address.*
  FROM parsed_customers;

-- COMMAND ----------

SELECT * FROM customers_final;

-- COMMAND ----------



-- COMMAND ----------



-- COMMAND ----------

SELECT order_id, customer_id, books FROM orders;

-- COMMAND ----------

DESCRIBE orders;

-- COMMAND ----------

SELECT order_id, 
customer_id, 
explode(books) AS book 
FROM orders WHERE order_id LIKE '%000000000004243%';

-- COMMAND ----------

SELECT customer_id,
  collect_set(order_id) AS orders_set,
  collect_set(books.book_id) AS books_set,
  collect_list(order_id) AS orders_list
FROM orders
GROUP BY customer_id;

-- COMMAND ----------

SELECT customer_id,
  collect_set(order_id) AS orders_set,
  flatten(collect_set(books.book_id)) AS books_set,
  array_distinct(flatten(collect_set(books.book_id))) AS unique_books_set,
  collect_list(order_id) AS orders_list
FROM orders
GROUP BY customer_id;

-- COMMAND ----------

CREATE OR REPLACE VIEW orders_enriched AS
SELECT * 
FROM (
  SELECT *, explode(books) AS book FROM orders
) o
INNER JOIN books b
on o.book.book_id = b.book_id;

SELECT * FROM orders_enriched;

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW orders_updates AS
SELECT * FROM PARQUET.`dbfs:/mnt/demo-datasets/bookstore/orders-new/`;

SELECT * FROM orders
UNION
SELECT * FROM orders_updates;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC files = dbutils.fs.ls('dbfs:/mnt/demo-datasets/bookstore/orders-new')
-- MAGIC display(files)

-- COMMAND ----------

SELECT * FROM orders
INTERSECT
SELECT * FROM orders_updates;

-- COMMAND ----------

SELECT * FROM orders
MINUS
SELECT * FROM orders_updates;

-- COMMAND ----------

CREATE OR REPLACE TABLE transactions AS
SELECT * FROM (
  SELECT customer_id,
    book.book_id AS book_id,
    book.quantity AS quantity
  FROM orders_enriched
)
PIVOT (
  sum(quantity) FOR book_id IN (
    'B01', 'B02', 'B03', 'B04', 'B05', 'B06', 
    'B07', 'B08', 'B09', 'B10', 'B11', 'B12'
  )
);

SELECT * FROM transactions;

-- COMMAND ----------

DESCRIBE EXTENDED orders_enriched;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql import functions as F
-- MAGIC (
-- MAGIC   spark
-- MAGIC     .read
-- MAGIC     .table('orders_enriched')
-- MAGIC     .groupBy('customer_id')
-- MAGIC     .pivot('book.book_id')
-- MAGIC     .agg(F.sum('book.quantity'))
-- MAGIC     .display()
-- MAGIC )

-- COMMAND ----------


