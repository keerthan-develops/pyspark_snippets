# Databricks notebook source
d1 = [('a', 'b', 'c')]
schema1 = ('col1', 'col2', 'col3')
spark.createDataFrame(data=d1, schema=schema1)



# COMMAND ----------

rdd = spark.sparkContext.parallelize(d1)
print(rdd.collect())
rdd_explode = rdd.flatMap(lambda x: x[0].split(','))
rdd_explode.collect()

# COMMAND ----------

