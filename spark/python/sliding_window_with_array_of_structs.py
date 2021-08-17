# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC https://stackoverflow.com/questions/68805893/pyspark-explode-array-column-into-sublist-with-sliding-window/68809510

# COMMAND ----------

import pyspark.sql.functions as f

# COMMAND ----------

input_df = spark.createDataFrame([
    (2,[1,2,3,4,5])
], ("id", "list"))

input_df.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Hard coding `N` value

# COMMAND ----------

output_df = (input_df
             .withColumn('list', f.expr('TRANSFORM(list, (element, i) -> STRUCT(ARRAY(COALESCE(list[i - 2], 0), COALESCE(list[i - 1], 0)) AS past, element AS future))'))
             .selectExpr('id', 'inline(list)'))

output_df.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Passing `N` dynamically

# COMMAND ----------

N = 2

expr = 'TRANSFORM(list, (element, i) -> STRUCT(TRANSFORM(sequence({N}, 1), k -> COALESCE(list[i - k], 0)) AS past, element AS future))'.format(N=N)
output_df = (input_df
             .withColumn('list', f.expr(expr))
             .selectExpr('id', 'inline(list)'))

output_df.show(truncate=False)
