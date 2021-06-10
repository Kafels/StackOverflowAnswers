# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC https://stackoverflow.com/questions/47364686/pyspark-cumulative-sum-with-reset-condition

# COMMAND ----------

df = spark.createDataFrame([(1, 5), (1, 4), (1, 3), (1, 5), (1, 6), (1, 4),
                            (1, 7), (1, 5), (1, 2), (1, 3), (1, 2), (1, 6), (1, 9)], ('Flag', 'value'))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # DataFrame

# COMMAND ----------

from pyspark.sql.window import Window

import pyspark.sql.functions as f

# COMMAND ----------

w = Window.partitionBy('flag').orderBy(f.monotonically_increasing_id()).rowsBetween(Window.unboundedPreceding, Window.currentRow)
df = df.withColumn('flag_values', f.collect_list('value').over(w).cast('array<int>'))

# COMMAND ----------

expr = "REDUCE(flag_values, 0, (c, n) -> IF(c < 20, c + n, n))"
df = df.select('Flag', 'value', f.expr(expr).alias('cumsum'))

df.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # RDD

# COMMAND ----------

df = spark.createDataFrame([(1, 5), (1, 4), (1, 3), (1, 5), (1, 6), (1, 4),
                            (1, 7), (1, 5), (1, 2), (1, 3), (1, 2), (1, 6), (1, 9)], ('Flag', 'value'))

# COMMAND ----------

def cumsum_by_flag(rows):
  cumsum, reset = 0, False
  for row in rows:
    cumsum += row.value
    
    if reset:
      cumsum = row.value
      reset = False
      
    reset = cumsum > 20
    yield row.value, cumsum
    
    
def unpack(value):
  flag = value[0]
  value, cumsum = value[1]
  return flag, value, cumsum

# COMMAND ----------

rdd = df.rdd.keyBy(lambda row: row.Flag)
rdd = (rdd
       .groupByKey()
       .flatMapValues(cumsum_by_flag)
       .map(unpack))

df = rdd.toDF('Flag int, value long, cumsum int')
df.show(truncate=False)
