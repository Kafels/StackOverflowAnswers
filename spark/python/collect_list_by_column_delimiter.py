# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC https://stackoverflow.com/questions/67593030/leag-lag-and-window-function-with-concat-function/67595008

# COMMAND ----------

from pyspark.sql import functions as f, Row
from pyspark.sql.window import Window

# COMMAND ----------

df = spark.createDataFrame([
  Row(Value='A', LineNumber=6),
  Row(Value='B', LineNumber=7),
  Row(Value='C', LineNumber=8),
  Row(Value='D|#|', LineNumber=9),
  Row(Value='A|#|', LineNumber=10),
  Row(Value='E', LineNumber=11),
  Row(Value='F', LineNumber=12),
  Row(Value='G|#|', LineNumber=13),
  Row(Value='I', LineNumber=23),
  Row(Value='J', LineNumber=24),
  Row(Value='K', LineNumber=25),
  Row(Value='L', LineNumber=25)
])

# COMMAND ----------

df = df.withColumn('filename', f.input_file_name())
df = df.repartition('filename')

# COMMAND ----------

# Creating an id to enable window functions
df = df.withColumn('index', f.monotonically_increasing_id())

# COMMAND ----------

w = Window.partitionBy('filename').orderBy('index')

# Identifying if the previous row has |#| delimiter
df = df.withColumn('delimiter', f.lag('Value', default=False).over(w).contains('|#|'))

# Creating a column to group all values that must be concatenated
df = df.withColumn('group', f.sum(f.col('delimiter').cast('int')).over(w))

# COMMAND ----------

# Grouping them, removing |#|, collecting all values and concatenate them
df = (df
      .groupBy('group')
      .agg(f.concat_ws(',', f.collect_list(f.regexp_replace('Value', '\|#\|', ''))).alias('ConcalValue'),
           f.min('LineNumber').alias('LineNumber')))

# COMMAND ----------

# Selecting only desired columns
(df
 .select(f.col('ConcalValue').alias('Concal Value'), f.col('LineNumber').alias('Initial Line Number'))
 .sort('LineNumber')
 .show(truncate=False))
