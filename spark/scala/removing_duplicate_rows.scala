// Databricks notebook source
// MAGIC %md
// MAGIC 
// MAGIC https://stackoverflow.com/questions/67586645/in-apache-spark-java-how-can-i-remove-elements-from-a-dataset-where-some-field-d/67594733

// COMMAND ----------

import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.{Window}

// COMMAND ----------

val df = sc.parallelize(Seq(
  (1, 2),
  (2, 2),
  (2, 3)
)).toDF("a", "b")

// COMMAND ----------

val window = Window.partitionBy("a")

val newDF = (df
        .withColumn("count", count(lit(1)).over(window))
        .where(col("count") === lit(1))
        .drop("count"))

newDF.show

// COMMAND ----------

display(newDF)
