# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC https://stackoverflow.com/questions/67621798/how-to-convert-arraystruct-to-arraystring/67622567

# COMMAND ----------

from pyspark.sql import Row
import pyspark.sql.functions as f

# COMMAND ----------

df = spark.createDataFrame([
  Row(ColA=[{"id": "1", "name": "Hello"}, {"id": "2", "name": "World"}])
])

# COMMAND ----------

expr = "TRANSFORM(ColA, x -> struct(x.id, x.name))"
df = df.withColumn('ColA', f.expr(expr))

# COMMAND ----------

expr = "TRANSFORM(ColA, x -> to_json(x))"
df = df.withColumn('ColA', f.expr(expr))

df.printSchema()
df.show(truncate=False)
