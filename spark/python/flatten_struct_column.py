# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC https://stackoverflow.com/questions/67400307/how-to-extract-a-json-string-from-a-column-in-spark/67443128

# COMMAND ----------

from pyspark import Row
from pyspark.sql import DataFrame

import pyspark.sql.functions as f

# COMMAND ----------

def get_schema(dataframe: DataFrame, column: str):
    row = dataframe.where(f.col(column).isNotNull()).select(column).first()
    return f.schema_of_json(f.lit(row.asDict()[column]))

# COMMAND ----------

def flatten(dataframe, column):
    # Adapted from https://stackoverflow.com/a/49532496/6080276 answer
    while True:
        nested_cols = [col for col, _type in dataframe.dtypes
                       if col.startswith(column) and _type.startswith('struct')]
        if len(nested_cols) == 0:
            break

        flat_cols = [col for col in dataframe.columns if col not in nested_cols]
        dataframe = dataframe.select(flat_cols +
                                     [f.col(nc + '.' + c).alias(nc + '_' + c)
                                      for nc in nested_cols
                                      for c in dataframe.select(nc + '.*').columns])
    return dataframe

# COMMAND ----------

def extract_json(dataframe, column_name):
    schema = get_schema(dataframe, column_name)
    dataframe = dataframe.withColumn(column_name, f.from_json(column_name, schema).alias(column_name))
    return flatten(dataframe, column_name)

# COMMAND ----------

df: DataFrame = spark.createDataFrame([
    Row(json_column='{"address": {"line1": "Test street","houseNumber": 123,"city": "New York"}, "name": "Test1"}',
        another_json='{"address": {"line1": "Test street","houseNumber": 123,"city": "New York"}, "name": "Test1"}'),
    Row(json_column='{"address": {"line1": "Test street","houseNumber": 456,"city": "Los Angeles"}, "name": "Test2"}',
        another_json='{"address": {"line1": "Test street","houseNumber": 456,"city": "Los Angeles"}, "name": "Test2"}')
])

display(df)

# COMMAND ----------

df = extract_json(dataframe=df, column_name='json_column')
display(df)

# COMMAND ----------

df = extract_json(dataframe=df, column_name='another_json')
display(df)
