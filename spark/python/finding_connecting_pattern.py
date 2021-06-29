# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC https://stackoverflow.com/questions/67748173/finding-connecting-pattern-adv-partition-to-get-latest-value-using-pyspark/67974753#67974753

# COMMAND ----------

import pyspark.sql.functions as f
from pyspark.sql import Window

# COMMAND ----------

df = spark.createDataFrame([
    ('Z2146', 'y3498'),
    ('A2286', 'C4955'),
    ('y3498', 'B5216'),
    ('B5216', 'B6849'),
    ('D7777', 'E8849'),
    ('C4955', 'Q7658'),
    ('B6849', 'C9965'),
], ('var_x', 'var_y'))

# COMMAND ----------

# Creating an incremental id to use later
df = df.withColumn('increasing_id', f.monotonically_increasing_id())

# Set to this column a value that would partition your data. If you don't have,
# leave as 1, but may have performance issues depending your dataset size
df = df.withColumn('partition', f.lit(1))


# COMMAND ----------

def transform(rows):
    mapper, group_id = {}, 0
    for row in rows:
        increasing_id, var_x, var_y = row

        if var_x in mapper:
            group = mapper.get(var_x)
            yield increasing_id, var_x, var_y, group
            mapper.pop(var_x)
            mapper[var_y] = group
        else:
            mapper[var_y] = group_id
            yield increasing_id, var_x, var_y, group_id
            group_id += 1


# COMMAND ----------

# Creating a new column with `RDD` that contains which group your row belongs to
df_linked = (df
             .rdd
             .map(lambda row: (row.partition, (row.increasing_id, row.var_x, row.var_y)))
             .groupByKey()
             .flatMapValues(transform)
             .map(lambda row: row[1])
             .toDF('increasing_id long, var_x string, var_y string, group int'))

# COMMAND ----------

# Getting the last `var_y` from each group
w = (Window.partitionBy('group')
     .orderBy(f.col('increasing_id').asc())
     .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing))
df_latest = df_linked.select('var_x', 'var_y', f.last('var_y').over(w).alias('Latest'))
df_latest.orderBy('group', 'increasing_id').show()
