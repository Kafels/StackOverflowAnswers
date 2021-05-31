// Databricks notebook source
// MAGIC %md
// MAGIC 
// MAGIC https://stackoverflow.com/questions/67763299/spark-dataframe-transformation-to-get-counts-of-a-particular-value-in-a-column/67763749

// COMMAND ----------

val df = sc.parallelize(Seq(
  ("on", "off"),
  ("off", "on"),
  ("on", "nc"),
  ("nc", "off")
)).toDF("A", "B")

// COMMAND ----------

val dfStack = df.selectExpr("""stack(2, "A", A, "B", B) as (features, value)""")
dfStack.show(false)

// COMMAND ----------

val dfGroup = dfStack.groupBy("features").pivot("value", Seq("on", "off", "nc")).count()
dfGroup.sort("features").show(false)
