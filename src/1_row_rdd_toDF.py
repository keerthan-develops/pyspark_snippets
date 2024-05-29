# Databricks notebook source
print('Jai Hanuman')

# COMMAND ----------

from pyspark.sql import Row
tuple1 = ('a', 'b', 'c')

# COMMAND ----------

rdd = spark.sparkContext.parallelize(tuple1)
rdd.collect()

# COMMAND ----------

row1 = Row(name='Jai', height='6')
row1.name

# COMMAND ----------

spark.createDataFrame([row1])

# COMMAND ----------

# Outer list is the taker of rows(consists of rows)
# Inner list is the taker of columns (consists of columns)
df = spark.createDataFrame([
    [2, "Alice"], [5, "Bob"]], schema=["age", "name"])
df.show(100, False)

# COMMAND ----------

df2 = spark.createDataFrame((
    (2, "Alice"), (5, "Bob")), schema=("age", "name"))
df2.show(100, False)


# COMMAND ----------

