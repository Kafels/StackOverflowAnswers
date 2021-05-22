# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC https://stackoverflow.com/questions/67628097/in-pyspark-i-need-to-transform-a-column-list-of-maps-into-a-string-within-a/67628654

# COMMAND ----------

from pyspark.sql import functions as f, Row

# COMMAND ----------

df = spark.createDataFrame([
  Row(random_column=[
    {
      "abc": "..."
    },
    {
      "def": "..."
    },
    {
      "ghi": "..."
    }
  ]
  )
])

# COMMAND ----------

(df
 .withColumn('random_column_keys', f.expr("REDUCE(random_column, cast(array() as array<string>), (acc, el) -> array_union(acc, map_keys(el)), acc -> concat_ws(',', acc))"))
 .show(truncate=False))
