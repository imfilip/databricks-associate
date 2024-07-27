# Databricks notebook source
!pip install faker

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

from faker import Faker
import pandas as pd
import random

def create_rows_faker(path = "user/mnt/csv_files/"):
    fake = Faker()
    first_file = len(dbutils.fs.ls(f"dbfs:/{path}")) == 0
    if first_file:
        file_name = str(0)
    else:
        file_name = len(dbutils.fs.ls(f"dbfs:/{path}"))
    output = [{
        "id": file_name,
        "name":fake.name(),
        # "address":fake.address(),
        "email":fake.email(),
        "city":fake.city(),
        "state":fake.state(),
        "date_time":fake.date_time(),
        "randomdata":random.randint(1000,2000)}]
    output = pd.DataFrame(output)
    output.to_csv(f"/dbfs/{path}{file_name}.csv", header=True, sep=';', index=False)
    return output

def erase_catalog(path):
    dbutils.fs.rm(path, recurse=True)
    dbutils.fs.mkdirs(path)
    create_rows_faker(path=path)

# COMMAND ----------

path = "user/mnt/csv_files/"
erase_catalog(path=path)

# COMMAND ----------

dbutils.fs.ls(f"dbfs:/{path}")

# COMMAND ----------

import time
for i in range(100):
    create_rows_faker(path = path)
    time.sleep(10)

# COMMAND ----------


