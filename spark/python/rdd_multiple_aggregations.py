# Databricks notebook source
data = sc.parallelize([('a', 110), ('a', 120), ('a', 130), ('b', 200), ('b', 206)])


# COMMAND ----------

def sequence_operator(initial_value, element):
    return (initial_value, element, element, element)


def combination_operator(prev, _next):
    return (prev[0] + _next[0],
            prev[1] + _next[1],
            min(prev[2], _next[2]),
            max(prev[3], _next[3]))


def unpack_aggregations(data):
    key = data[0]
    count, _sum, _min, _max = data[1]
    return key, count, _sum / count, _min, _max


# COMMAND ----------

aggregations = data.aggregateByKey(zeroValue=1, seqFunc=sequence_operator, combFunc=combination_operator)
mapped_data = aggregations.map(unpack_aggregations)

# COMMAND ----------

print(mapped_data.collect())
