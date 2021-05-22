# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC https://stackoverflow.com/questions/67533261/merge-concatenate-an-array-of-maps-into-one-map-in-spark-sql-with-built-ins/67534547

# COMMAND ----------

import pyspark.sql.functions as F

# COMMAND ----------

df = spark.createDataFrame([
    (1, [{"alpha": "beta"}, {"gamma": "delta"}]),
    (2, [{"epsilon": "zeta"}, {"etha": "theta"}])
],
    schema=["id", "greek"]
)

# COMMAND ----------

map_schema = df.selectExpr('greek[0]').dtypes[0][1]
map_schema

# COMMAND ----------

expr = "REDUCE(greek, cast(map() as {schema}), (acc, el) -> map_concat(acc, el))".format(schema=map_schema)
df = df.withColumn("Concated", F.expr(expr))

# COMMAND ----------

df.show(truncate=False)
