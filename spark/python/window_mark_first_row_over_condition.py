# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC https://stackoverflow.com/questions/68235518/pyspark-window-function-mark-first-row-of-each-partition-that-meet-specific-cond/68246808#68246808

# COMMAND ----------

from pyspark.sql import Row, Window
import pyspark.sql.functions as f

# COMMAND ----------

df = spark.createDataFrame([
  Row(app_id='AP-1', order=1, entry_flag=1, operator='S'),
  Row(app_id='AP-1', order=2, entry_flag=0, operator='A'),
  Row(app_id='AP-2', order=3, entry_flag=0, operator='S'),
  Row(app_id='AP-2', order=4, entry_flag=0, operator='A'),
  Row(app_id='AP-2', order=5, entry_flag=1, operator='S'),
  Row(app_id='AP-2', order=6, entry_flag=0, operator='S'),
  Row(app_id='AP-2', order=7, entry_flag=0, operator='A'),
  Row(app_id='AP-2', order=8, entry_flag=0, operator='A'),
  Row(app_id='AP-2', order=9, entry_flag=1, operator='A'),
  Row(app_id='AP-2', order=10, entry_flag=0, operator='S')
])

# COMMAND ----------

# Creating a column to group each entry where the value is 1
w_entry = Window.partitionBy('app_id').orderBy('order')
df = df.withColumn('group', f.sum('entry_flag').over(w_entry))

# Applying your boolean rule
df = df.withColumn('match', f.when(f.col('group') > f.lit(0), 
                                   (f.col('entry_flag') == f.lit(0)) & (f.col('operator')== f.lit('A')))
                             .otherwise(f.lit(False)))

# COMMAND ----------

# If a group has two or more matches like the example below
# |AP-2  |7    |0         |A       |1    |true |
# |AP-2  |8    |0         |A       |1    |true |
# identify which is the first occurrence and set `flag_x` with 1 to it.

w_flag = Window.partitionBy('app_id', 'group', 'match')
df = df.withColumn('flag_x', (f.col('match') & (f.col('order') == f.min('order').over(w_flag))).cast('int'))

# COMMAND ----------

# Drop temporary columns
df = df.drop('group', 'match')

# COMMAND ----------

df.show(truncate=False)
