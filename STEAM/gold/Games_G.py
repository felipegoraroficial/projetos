# Databricks notebook source
# Importação das funções necessárias do PySpark
from pyspark.sql.functions import when, col
from pyspark.sql.types import StringType

# Leitura do arquivo CSV com os dados dos jogos da Steam
df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("Files/Silver/steam_games.csv")

# Leitura do arquivo CSV com os detalhes dos jogos da Steam
df1 = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("Files/Silver/steam_details.csv")

# Junção dos dois DataFrames com base na coluna de ID dos jogos
df = df.join(df1, df.id == df1.steam_appid, 'inner').select(df["*"], df1["website"], df1["minimum"], df1["recommended"])

# Seleção das colunas de tipo String
string_cols = [f.name for f in df.schema.fields if f.dataType == StringType()]

# Tratamento de valores nulos e 'nan' nas colunas de tipo String
for col_name in string_cols:
    df = df.withColumn(col_name, when(col(col_name).isNull(), '-').otherwise(col(col_name)))
    df = df.withColumn(col_name, when(col(col_name) == 'nan', '-').otherwise(col(col_name)))

# Exibição dos dados transformados
display(df)

# Escrita dos dados transformados em uma tabela Delta
df.write.format("delta").mode("overwrite").saveAsTable("steam_games")

