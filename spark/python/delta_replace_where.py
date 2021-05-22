# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC https://stackoverflow.com/questions/67540077/would-replacewhere-causes-deletion/67542956

# COMMAND ----------

import pyspark.sql.functions as f
from pyspark.sql import Row

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DROP TABLE IF EXISTS my_delta_table

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Creating 1000 rows partitioned by `even` column

# COMMAND ----------

rows = [Row(number=i) for i in range(0, 1000)]

df = spark.createDataFrame(rows)
df = df.withColumn('even', (f.col('number') % 2 == f.lit(0)).cast('int'))

(df
 .write
 .partitionBy('even')
 .format('delta')
 .saveAsTable('my_delta_table'))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Creating 10 rows and filtering only `even` values

# COMMAND ----------

rows = [Row(number=i) for i in range(0, 10)]

df_only_even = spark.createDataFrame(rows)
df_only_even = df_only_even.withColumn('even', (f.col('number') % 2 == f.lit(0)).cast('int'))

# It is required to filter your dataframe or will throw an error during write operation
df_only_even = df_only_even.where(f.col('even') == f.lit(1))
display(df_only_even)

# COMMAND ----------

(df_only_even
 .write
 .partitionBy('even')
 .format('delta')
 .option('replaceWhere', 'even == 1')
 .mode('overwrite')
 .saveAsTable('my_delta_table'))

# COMMAND ----------

display(spark
        .table('my_delta_table')
        .sort(f.col('even').desc(), f.col('number')))
