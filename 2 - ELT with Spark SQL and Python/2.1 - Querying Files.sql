-- Databricks notebook source
-- MAGIC %run ../Includes/Copy-Datasets

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dataset_bookstore

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/mnt/demo-datasets/bookstore

-- COMMAND ----------

-- MAGIC %python
-- MAGIC files = dbutils.fs.ls(f"{dataset_bookstore}/books-csv")
-- MAGIC display(files)

-- COMMAND ----------

-- SELECT * FROM json.`dbfs:/mnt/demo-datasets/bookstore/customers-json/export_001.json`;
SELECT * FROM json.`${dataset.bookstore}/customers-json/export_001.json`

-- COMMAND ----------

SELECT * FROM json.`${dataset.bookstore}/customers-json/export_*.json`

-- COMMAND ----------

SELECT * FROM json.`${dataset.bookstore}/customers-json`

-- COMMAND ----------

SELECT count(*) FROM json.`${dataset.bookstore}/customers-json`

-- COMMAND ----------

SELECT count(distinct customer_id) FROM json.`${dataset.bookstore}/customers-json`

-- COMMAND ----------

SELECT input_file_name() source_file, * FROM json.`${dataset.bookstore}/customers-json`;

-- COMMAND ----------

SELECT *, input_file_name() source_file FROM text.`${dataset.bookstore}/customers-json`

-- COMMAND ----------

SELECT *, input_file_name() source_file FROM binaryFile.`${dataset.bookstore}/customers-json`

-- COMMAND ----------

SELECT 
  *, 
  input_file_name() source_file 
FROM csv.`${dataset.bookstore}/books-csv`
;

-- COMMAND ----------

USE imfilip;
CREATE TABLE IF NOT EXISTS books_csv
  (book_id STRING, title STRING, author STRING, category STRING, price DOUBLE)
USING CSV
OPTIONS (
  header="true",
  delimiter=";"
)
LOCATION "dbfs:/mnt/demo-datasets/bookstore/books-csv/"

-- COMMAND ----------

SELECT * FROM books_csv;

-- COMMAND ----------

SHOW TABLES;

-- COMMAND ----------

DESCRIBE EXTENDED books_csv;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC files = dbutils.fs.ls(f"{dataset_bookstore}/books-csv")
-- MAGIC display(files)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC (
-- MAGIC     spark
-- MAGIC         .read
-- MAGIC             .table("books_csv")
-- MAGIC             .coalesce(1)
-- MAGIC         .write
-- MAGIC             .mode("append")
-- MAGIC             .format("csv")
-- MAGIC             .option("header", True)
-- MAGIC             .option("delimiter", ";")
-- MAGIC             .save(f"{dataset_bookstore}/books-csv")
-- MAGIC )

-- COMMAND ----------

-- MAGIC %python
-- MAGIC files = dbutils.fs.ls(f"{dataset_bookstore}/books-csv")
-- MAGIC display(files)

-- COMMAND ----------

SELECT count(*) FROM books_csv;

-- COMMAND ----------

REFRESH TABLE books_csv;

-- COMMAND ----------

SELECT count(*) FROM books_csv;

-- COMMAND ----------

CREATE TABLE customers
AS SELECT * FROM json.`dbfs:/mnt/demo-datasets/bookstore/customers-json/`;

DESCRIBE EXTENDED customers;

-- COMMAND ----------

CREATE TABLE books_unparsed
AS SELECT * FROM csv.`dbfs:/mnt/demo-datasets/bookstore/books-csv/`;

DESCRIBE EXTENDED books_unparsed;

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW books_tmp_vw
  (book_id STRING, title STRING, author STRING, category STRING, price DOUBLE)
USING CSV
OPTIONS (
  path="dbfs:/mnt/demo-datasets/bookstore/books-csv/export*",
  header="true",
  delimiter=";"
);

-- COMMAND ----------

SELECT * FROM books_tmp_vw;

-- COMMAND ----------

CREATE OR REPLACE TABLE books AS
  SELECT * FROM books_tmp_vw;

SELECT * FROM books;

-- COMMAND ----------

DESCRIBE EXTENDED books;

-- COMMAND ----------

SELECT * FROM csv.`dbfs:/mnt/demo-datasets/bookstore/books-csv/export*`;

CREATE OR REPLACE TEMP VIEW books_tmp_view_2
  (book_id STRING, title STRING, author STRING, category STRING, price DOUBLE)
USING CSV
OPTIONS (
  path = 'dbfs:/mnt/demo-datasets/bookstore/books-csv/export*',
  delimiter = '.',
  header = True
);

-- COMMAND ----------

DESCRIBE EXTENDED books_tmp_view_2;

-- COMMAND ----------

CREATE OR REPLACE TABLE books_2 AS
SELECT * FROM books_tmp_view_2;

-- COMMAND ----------

DESCRIBE EXTENDED books_2;

-- COMMAND ----------


