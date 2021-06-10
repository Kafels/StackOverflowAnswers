// Databricks notebook source
// MAGIC %md
// MAGIC 
// MAGIC https://stackoverflow.com/questions/67911902/nested-json-extract-the-value-with-unknown-key-in-the-middle/67913458#67913458

// COMMAND ----------

import org.apache.spark.sql.functions._

// COMMAND ----------

val sourceDf = sc.parallelize(Seq(
  ("""{"a":"value1","b":"value1","c":true,"details":{"qgiejfkfk123":{"model1":{"score":0.531,"version":"v1"},"model2":{"score":0.840,"version":"v2"},"other_details":{"decision":false,"version":"v1"}}}}""")
)).toDF("colJson")

// COMMAND ----------

val schema = "STRUCT<`details`: MAP<STRING, STRUCT<`model1`: STRUCT<`score`: DOUBLE, `version`: STRING>, `model2`: STRUCT<`score`: DOUBLE, `version`: STRING>, `other_details`: STRUCT<`decision`: BOOLEAN, `version`: STRING>>>>"

val fromJsonDf = sourceDf.withColumn("colJson", from_json(col("colJson"), lit(schema)))
val explodeDf = fromJsonDf.select($"*", explode(col("colJson.details")))

// COMMAND ----------

val finalDf = explodeDf.select(col("value.model1.score").as("model1_score"), col("value.model2.score").as("model2_score"))
finalDf.show
