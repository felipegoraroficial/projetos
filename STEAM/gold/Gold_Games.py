# Databricks notebook source
df = spark.read.format("csv").option("header","true").option("inferSchema", "true").load("dbfs:/silver/steam/games/steam_games.csv")

# COMMAND ----------

df.printSchema()

# COMMAND ----------

display(df)

# COMMAND ----------

df.write.format("delta").mode("overwrite").save('dbfs:/gold/steam/games/steam_games')
