// Databricks notebook source
// MAGIC %md
// MAGIC 
// MAGIC https://stackoverflow.com/questions/67510750/spark-dataframe-representing-schema-of-maptype-with-non-homogeneous-data-types/67521957

// COMMAND ----------

import org.apache.spark.sql.functions.{col, from_json}
import spark.implicits._

// COMMAND ----------

val df = sc.parallelize(Seq(
  ("""{"data":{"name":{"first":"john"},"address":{"street":"wall","zip":10000},"occupation":{"id":"12345","role":"Software Engineer"}}}"""),
  ("""{"data":{"name":{"first":"john"},"address":{"street":"wall","zip":10000}}}"""),
)).toDF("my_json_column")

// COMMAND ----------

val rows = df.select("my_json_column").as[String]
val schema = spark.read.json(rows).schema

// COMMAND ----------

// Transforming your String to Struct
val newDF = df.withColumn("obj", from_json(col("my_json_column"), schema))

newDF.printSchema
// root
//  |-- my_json_column: string (nullable = true)
//  |-- obj: struct (nullable = true)
//  |    |-- data: struct (nullable = true)
//  |    |    |-- address: struct (nullable = true)
//  |    |    |    |-- street: string (nullable = true)
//  |    |    |    |-- zip: long (nullable = true)
//  |    |    |-- name: struct (nullable = true)
//  |    |    |    |-- first: string (nullable = true)
//  |    |    |-- occupation: struct (nullable = true)
//  |    |    |    |-- id: string (nullable = true)
//  |    |    |    |-- role: string (nullable = true)

newDF.select("obj.data", "obj.data.occupation.id").show(false)
