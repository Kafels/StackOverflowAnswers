# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC https://stackoverflow.com/questions/67741851/pyspark-transform-key-value-pairs-into-columns

# COMMAND ----------

from pyspark.sql.types import *
import pyspark.sql.functions as f

# COMMAND ----------

schema = StructType(
    [
      StructField('Url', StringType(), True),
      StructField('Method', StringType(), True),
      StructField("Headers",ArrayType(StructType([
        StructField('Key', StringType(), True),
        StructField("Value",ArrayType(StringType()),True),
      ]),True),True)
    ]
  )

# COMMAND ----------

df = spark.read.format("json").load("my_data_path", schema=schema)

# COMMAND ----------

df = df.select('Url', 'Method', f.explode(f.expr('REDUCE(Headers, cast(map() as map<string, array<string>>), (acc, el) -> map_concat(acc, map(el.Key, el.Value)))')))

# COMMAND ----------

df_pivot = (df
            .groupBy('Url', 'Method')
            .pivot('key')
            .agg(f.first('value')))

# COMMAND ----------

for column, _type in df_pivot.dtypes:
  if _type.startswith('array'):
    df_pivot = df_pivot.withColumn(column, f.explode(column))

# COMMAND ----------

df_pivot.show(truncate=False)
