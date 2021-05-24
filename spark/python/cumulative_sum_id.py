# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC https://stackoverflow.com/questions/67661129/assign-unique-id-based-on-match-between-two-columns-in-pyspark-dataframe

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql import Row

import pyspark.sql.functions as f

# COMMAND ----------

df = spark.createDataFrame([
    Row(Column1=1, Column2=2, flag=True),
    Row(Column1=1, Column2=3, flag=True),
    Row(Column1=2, Column2=1, flag=True),
    Row(Column1=2, Column2=3, flag=True),
    Row(Column1=3, Column2=1, flag=True),
    Row(Column1=3, Column2=2, flag=True),
    Row(Column1=4, Column2=None, flag=False),
    Row(Column1=5, Column2=None, flag=False),
    Row(Column1=6, Column2=7, flag=True),
    Row(Column1=7, Column2=6, flag=True),
    Row(Column1=9, Column2=2, flag=True),
    Row(Column1=1, Column2=9, flag=True),
    Row(Column1=3, Column2=9, flag=True),
    Row(Column1=2, Column2=9, flag=True),
    Row(Column1=8, Column2=None, flag=False)
])

# COMMAND ----------

# Creating an index to retrieve original dataframe at the end
df = df.withColumn('index', f.monotonically_increasing_id())

# COMMAND ----------

w = Window.orderBy('least')

# Creating a column with least value from `Column1` and `Column2`. This will be used to "group" the values that must have the same ID
df = df.withColumn('least', f.least(f.col('Column1'), f.col('Column2')))

# Check if the current or previous `flag` is false to increase the id
df = df.withColumn('increase', ((~f.col('flag')) | (~f.lag('flag', default=True).over(w))).cast('int'))

# Generating incremental id
df = df.withColumn('ID', f.lit(101) + f.sum('increase').over(w))

# COMMAND ----------

(df
 .select('ID', 'Column1')
 .drop_duplicates()
 .sort('index')
 .show(truncate=False))
