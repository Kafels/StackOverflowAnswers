# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC https://stackoverflow.com/questions/68076603/aggregate-over-time-windows-on-a-partitioned-grouped-by-window/68086351

# COMMAND ----------

from pyspark.sql import Window

import pyspark.sql.functions as f

# COMMAND ----------

test_df = spark.createDataFrame([
  (2,3.0,"a", "2020-01-01"),
  (2,6.0,"a", "2020-01-02"),
  (3,2.0,"a", "2020-01-02"),
  (4,1.0,"b", "2020-01-04"),
  (4,9.0,"b", "2020-01-05"),
  (4,7.0,"b", "2020-01-05"),
  (2,3.0,"a", "2020-01-08"),
  (4,7.0,"b", "2020-01-09")
], ("id", "num","st", "date"))

# COMMAND ----------

window = Window.orderBy(f.col('date'))

before_dedup_df = (test_df
                   .withColumn('_custom_id', f.concat(f.col('id'), f.col('st')))
                   .withColumn('_consecutive', f.col('_custom_id').eqNullSafe(f.lag('_custom_id').over(window))))

before_dedup_df.show(truncate=False)

# COMMAND ----------

dedup_df = (before_dedup_df
            .where(~f.col('_consecutive'))
            .drop('_custom_id', '_consecutive'))

dedup_df.show(truncate=False)
