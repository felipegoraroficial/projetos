# Databricks notebook source
from pyspark.sql.functions import when, col, max, row_number
from pyspark.sql.window import Window
from pyspark.sql.types import StringType

# COMMAND ----------

df = spark.read.format("csv").option("header","true").option("inferSchema", "true").load("dbfs:/silver/steam/user/steam_user.csv")

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.write.format("delta").mode("overwrite").save('dbfs:/gold/steam/user/steam_user')

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS env_prod.steam

# COMMAND ----------

# Crar uma especificação de janela para particionar por appid e ordenar por file_data em ordem decrescente
window_spec = Window.partitionBy("appid").orderBy(col("file_data").desc())

# Adicionar uma coluna de número de linha para identificar as linhas mais recentes para cada appid
df = df.withColumn("row_number", row_number().over(window_spec))

display(df)

# COMMAND ----------

# Filtrar o dataframe para manter apenas as linhas com row_number = 1
df = df.filter(col("row_number") == 1)

# Eliminar a coluna row_number
df = df.drop("row_number")

display(df)

# COMMAND ----------

df.write.mode("overwrite").saveAsTable('steam.user')
