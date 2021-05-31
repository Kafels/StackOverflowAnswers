# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC https://stackoverflow.com/questions/47364686/pyspark-cumulative-sum-with-reset-condition

# COMMAND ----------

from pyspark.sql.window import Window

import pyspark.sql.functions as f

# COMMAND ----------

df = spark.createDataFrame([(1, 5), (1, 4), (1, 3), (1, 5), (1, 6), (1, 4),
                            (1, 7), (1, 5), (1, 2), (1, 3), (1, 2), (1, 6), (1, 9)], ('Flag', 'value'))

# COMMAND ----------

w = Window.partitionBy('flag').orderBy(f.monotonically_increasing_id()).rowsBetween(Window.unboundedPreceding, Window.currentRow)
df = df.withColumn('flag_values', f.collect_list('value').over(w).cast('array<int>'))

# COMMAND ----------

expr = "REDUCE(flag_values, 0, (c, n) -> IF(c < 20, c + n, n))"
df = df.select('flag', 'value', f.expr(expr).alias('cumsum'))

df.show(truncate=False)
