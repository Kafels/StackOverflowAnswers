# Databricks notebook source
data = sc.parallelize([('a', 110), ('a', 120), ('a', 130), ('b', 200), ('b', 206)])

# COMMAND ----------

def sequence_operator(accumulator, element):
  return (accumulator[0] + 1,
         accumulator[1] + element, 
         min(accumulator[2], element),
         max(accumulator[3], element))


def combination_operator(current_accumulator, next_accumulator):
  return (current_accumulator[0] + next_accumulator[0],
         current_accumulator[1] + next_accumulator[1], 
         min(current_accumulator[2], next_accumulator[2]),
         max(current_accumulator[3], next_accumulator[3]))


def unpack_aggregations(data):
  key = data[0]
  count, total, minimum, maximum = data[1]
  return key, count, total / count, minimum, maximum 

# COMMAND ----------

aggregations = data.aggregateByKey(zeroValue=(0, 0, float('inf'), float('-inf')), seqFunc=sequence_operator, combFunc=combination_operator)
aggregations.collect()

# COMMAND ----------

mapped_data = aggregations.map(unpack_aggregations)
mapped_data.collect()
