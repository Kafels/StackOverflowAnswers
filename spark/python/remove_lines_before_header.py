# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC https://stackoverflow.com/questions/67602156/find-specific-word-in-input-file-and-read-the-data-from-next-row-in-pyspark/67605060

# COMMAND ----------

from pyspark.sql.window import Window
import pyspark.sql.functions as f

# COMMAND ----------

df = (spark
      .read
      .format('csv')
      .option('delimiter', '|')
      .schema('CUST_ID string, CUST_NAME string, ORDER_NO integer, ORDER_ITEM STRING')
      .load('dbfs:/FileStore/tables/filter_rdd/data.txt'))

# COMMAND ----------

# Identifying which line is the header
df = (df
      .withColumn('id', f.monotonically_increasing_id())
      .withColumn('header', f.lag('CUST_ID', default=False).over(Window.orderBy('id')) == f.lit('EOH')))

# COMMAND ----------

# Collecting only header row to python context
header = df.where(f.col('header')).head()

# COMMAND ----------

# Removing all rows before header
df = (df
      .where(f.col('id') > f.lit(header.id))
      .drop('id', 'header'))

# COMMAND ----------

df.show()
