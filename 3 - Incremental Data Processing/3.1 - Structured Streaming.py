# Databricks notebook source
# MAGIC %run ../Includes/Copy-Datasets

# COMMAND ----------

(
    spark
    .readStream
    .table('imfilip.books')
    .createOrReplaceTempView("books_streaming_tmp_vw")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC USE imfilip;
# MAGIC SHOW TABLES;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM books_streaming_tmp_vw;

# COMMAND ----------

_sqldf.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO books
# MAGIC VALUES
# MAGIC   ("B1001", "Moja nowa ksiazka", "Marcin Filip", "Computer Science", 1000);

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT author, count(book_id) AS total_books
# MAGIC FROM books_streaming_tmp_vw
# MAGIC GROUP BY author;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW author_counts_tmp_vw AS (
# MAGIC   SELECT author, count(book_id) AS total_books
# MAGIC   FROM books_streaming_tmp_vw
# MAGIC   GROUP BY author 
# MAGIC );

# COMMAND ----------

(
    spark
        .table("author_counts_tmp_vw")
        .writeStream
        .trigger(processingTime='4 seconds')
        .outputMode("complete")
        .option("checkpointLocation", "dbfs:/mnt/demo/author_counts_checkpoint")
        .table("author_counts")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM author_counts;

# COMMAND ----------

(
    spark
        .readStream
        .option("checkpointLocation", "dbfs:/mnt/demo/author_counts_checkpoint")
        .table("author_counts")
        .display()
)

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO books
# MAGIC VALUES
# MAGIC   ("B1008", "Moja osma ksiazka", "Marcin Filip", "Computer Science", 8000);

# COMMAND ----------

(
    spark
        .table("author_counts_tmp_vw")
        .display()
)

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/demo/author_counts_checkpoint

# COMMAND ----------

(
    spark
        .table("author_counts_tmp_vw")
        .writeStream
        .trigger(availableNow=True)
        .outputMode("complete")
        .option("checkpointLocation", "dbfs:/mnt/demo/author_counts_checkpoint")
        .table("author_counts")
        .awaitTermination()
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM author_counts;

# COMMAND ----------

(
    spark
        .table("author_counts_tmp_vw")
        .write
        .mode('overwrite')
        .saveAsTable("author_counts2")
)

# COMMAND ----------

(
    spark
        .table("author_counts_tmp_vw")
        .isStreaming
)

# COMMAND ----------

(
    spark
        .table("books_streaming_tmp_vw")
        .isStreaming
)

# COMMAND ----------

(
    spark
        .table("author_counts")
        .isStreaming
)

# COMMAND ----------

(
    spark
        .table("books")
        .isStreaming
)

# COMMAND ----------

# MAGIC %md
# MAGIC
