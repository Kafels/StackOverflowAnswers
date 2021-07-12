# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC https://stackoverflow.com/questions/68287551/nested-json-from-rest-api-to-pyspark-dataframe/68288770

# COMMAND ----------

import requests

# COMMAND ----------

response = requests.get('http://api.citybik.es/v2/networks?fields=id,name,href')
rdd = spark.sparkContext.parallelize([response.text])

# COMMAND ----------

df = spark.read.json(rdd)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

(df
 .selectExpr('inline(networks)')
 .show(n=5, truncate=False))
