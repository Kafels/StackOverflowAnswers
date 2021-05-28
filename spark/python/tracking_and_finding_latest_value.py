# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC https://stackoverflow.com/questions/67738965/tracking-and-finding-latest-value-in-dataframe-using-pyspark/67739944#67739944

# COMMAND ----------

from pyspark.sql import Row
from pyspark.sql.window import Window

import pyspark.sql.functions as f

# COMMAND ----------

df = spark.createDataFrame([
  (10, 12),
  (12, 14),
  (14, 16),
  (18, 19),
  (19, 20),
  (22, 24),
  (24, 25),
  (25, 27),
  (29, 30)
], ('obsolute', 'replace'))

# COMMAND ----------

w = Window.orderBy('obsolute')

# COMMAND ----------

df = (df
      .withColumn('chain', f.coalesce(f.lag('replace').over(w) == f.col('obsolute'), f.lit(True)))
      .withColumn('group', f.sum((f.col('chain') == f.lit(False)).cast('Int')).over(w)))

# COMMAND ----------

w = Window.partitionBy('group')
df = df.select('obsolute', 'replace', f.last('replace').over(w).alias('latest'))

# COMMAND ----------

df.show(truncate=False)
