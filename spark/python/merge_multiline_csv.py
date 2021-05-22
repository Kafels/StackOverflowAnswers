# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC https://stackoverflow.com/questions/67579000/merge-multiline-records-using-pyspark/67635755

# COMMAND ----------

from pyspark.sql.window import Window
import pyspark.sql.functions as f

# COMMAND ----------

df = spark.read.csv('dbfs:/FileStore/tables/merge_multiline/', schema='value string')
df = df.withColumn('filename', f.input_file_name())
df = df.repartition('filename')

# COMMAND ----------

df = df.withColumn('index', f.monotonically_increasing_id())

w = Window.partitionBy('filename').orderBy('index')
df = df.withColumn('group', f.sum(f.lag('value', default=False).over(w).endswith(f.lit('|##|')).cast('int')).over(w))

# COMMAND ----------

df = df.withColumn('value', f.regexp_replace('value', '\|\*\|', '~'))
df = df.withColumn('value', f.regexp_replace('value', '\|##\|', ''))

# COMMAND ----------

df = df.groupBy('group').agg(f.concat_ws('', f.collect_list('value')).alias('value'))

# COMMAND ----------

(df
 .select('value')
 .sort('group')
 .show(truncate=False))
