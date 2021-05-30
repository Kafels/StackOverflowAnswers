# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC https://stackoverflow.com/questions/67705881/unable-to-read-images-simultaneously-in-parallels-using-pyspark/67745881#67745881

# COMMAND ----------

from pyspark.ml.image import ImageSchema
import numpy as np

# COMMAND ----------

df = (spark
      .read
      .format("image")
      .option("pathGlobFilter", "*.jpg")
      .load("path"))

df = df.select('image.*')

# COMMAND ----------

# Pre-caching the required schema. If you remove this line an error will be raised.
ImageSchema.imageFields

# Transforming images to np.array
arrays = df.rdd.map(ImageSchema.toNDArray).collect()

# COMMAND ----------

img = np.array(arrays)
print(img.shape)
