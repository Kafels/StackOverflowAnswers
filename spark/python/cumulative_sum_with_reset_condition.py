# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC https://stackoverflow.com/questions/47364686/pyspark-cumulative-sum-with-reset-condition

# COMMAND ----------

from pyspark.sql import Row
from pyspark.sql.window import Window

import pyspark.sql.functions as f

# COMMAND ----------

df = spark.createDataFrame(
    [Row(Flag=1, value=5), Row(Flag=1, value=4), Row(Flag=1, value=3), Row(Flag=1, value=5), Row(Flag=1, value=6),
     Row(Flag=1, value=4), Row(Flag=1, value=7), Row(Flag=1, value=5), Row(Flag=1, value=2), Row(Flag=1, value=3),
     Row(Flag=1, value=2), Row(Flag=1, value=6), Row(Flag=1, value=9)]
)

display(df)

# COMMAND ----------

window = Window.partitionBy('flag')
df = df.withColumn('row_id', f.row_number().over(window.orderBy('flag')).cast('int'))
df = df.withColumn('values', f.collect_list('value').over(window).cast('array<int>'))

display(df)

# COMMAND ----------

expr = "TRANSFORM(slice(values, 1, row_id), sliced_array -> sliced_array)"
df = df.withColumn('sliced_array', f.expr(expr))

display(df)

# COMMAND ----------

expr = "REDUCE(sliced_array, 0, (c, n) -> IF(c < 20, c + n, n))"
df = df.select('flag', 'value', f.expr(expr).alias('cumsum'))

display(df)
