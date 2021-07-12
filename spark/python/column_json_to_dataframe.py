# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC https://stackoverflow.com/questions/68352430/azure-databricks-python-convert-json-column-string-to-dataframe/68354011

# COMMAND ----------

import pyspark.sql.functions as f

# COMMAND ----------

df = spark.createDataFrame([
  ["""{"Timestamp":3690414400,"Sender":"10.99.45.6:32768:wifivm0002EF","Type":"1.3.6.1.4.1.9.9.599.0.8","CaptureTime":637616722902708244,"Variables":[{"Key":"1.3.6.1.4.1.9.9.513.1.2.1.1.1.0","Value":"1"},{"Key":"1.3.6.1.4.1.9.9.513.1.1.1.1.5.200.249.249.41.0.128","Value":{"Hex":"66696E7362792D7761703033","String":"123456-wap03"}},{"Key":"1.3.6.1.4.1.9.9.599.1.3.2.1.2.0","Value":1},{"Key":"1.3.6.1.4.1.9.9.599.1.3.2.1.3.0","Value":{"Hex":"0A9603F4","String":"\n?\u0003?"}},{"Key":"1.3.6.1.4.1.9.9.599.1.3.1.1.27.114.154.56.22.154.160","Value":{"Hex":"766D6564776966692F646965676F33756B407961686F6F2E636F6D","String":"vmedwifi/xxxuk@yahoo.com"}},{"Key":"1.3.6.1.4.1.9.9.599.1.3.1.1.28.114.154.56.22.154.160","Value":{"Hex":"56697267696E204D65646961","String":"Virgin Media"}},{"Key":"1.3.6.1.4.1.9.9.599.1.3.1.1.38.114.154.56.22.154.160","Value":{"Hex":"36306562663133322F37323A39613A33383A31363A39613A61302F3931323639363136","String":"60ebf132/72:9a:38:16:9a:a0/91269616"}},{"Key":"1.3.6.1.4.1.9.9.599.1.3.1.1.8.114.154.56.22.154.160","Value":{"Hex":"C8F9F9290080","String":"???)\u0000?"}}]}"""]
], schema='BodyJson string')

# COMMAND ----------

rdd = df.rdd.map(lambda row: row.BodyJson)
body_df = spark.read.json(rdd, allowUnquotedControlChars=True)

# COMMAND ----------

variables_df = body_df.selectExpr('inline(Variables)')
variables_df = variables_df.withColumn('ObjValue', f.get_json_object('Value', '$.String'))

# COMMAND ----------

form_1_df = variables_df.where(f.col('ObjValue').isNull())
form_1_df = form_1_df.drop('ObjValue')
display(form_1_df)

# COMMAND ----------

form_2_df = variables_df.where(f.col('ObjValue').isNotNull())
form_2_df = form_2_df.select('Key', f.col('ObjValue').alias('Value'))
display(form_2_df)
