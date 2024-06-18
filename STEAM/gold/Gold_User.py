# Databricks notebook source
from pyspark.sql.functions import when, col
from pyspark.sql.types import StringType

# COMMAND ----------

df = spark.read.format("csv").option("header","true").option("inferSchema", "true").load("dbfs:/silver/steam/user/steam_user.csv")

# COMMAND ----------

df.printSchema()

# COMMAND ----------

display(df)

# COMMAND ----------

df.write.format("delta").mode("overwrite").save('dbfs:/gold/steam/user/steam_user')
