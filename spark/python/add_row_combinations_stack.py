# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC https://stackoverflow.com/questions/67594989/better-way-to-add-column-values-combinations-to-data-frame-in-pyspark/67595227

# COMMAND ----------

import pyspark.sql.functions as f

# COMMAND ----------

# Simplified version of my data frame
data = [("1", "2020-04-01", 5), 
        ("2", "2020-04-01", 5), 
        ("3", "2020-04-02", 4)]
df = spark.createDataFrame(data, ['id','day', 'value'])

# COMMAND ----------

df_days = df.select(f.col('day').alias('r_day')).distinct()

# COMMAND ----------

df_final = df.join(df_days, on=df['day'] != df_days['r_day'])

# COMMAND ----------

df_final = df_final.select('id', f.expr('stack(2, day, value, r_day, cast(0 as bigint)) as (day, value)'))
df_final.orderBy('id','day').show()
