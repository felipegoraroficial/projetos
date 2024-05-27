# Databricks notebook source
# Importação das funções necessárias do PySpark
from pyspark.sql.functions import explode, col, round
from pyspark.sql.types import StructType, StructField, StringType, FloatType

# Definição do esquema para os dados de jogos do usuário na Steam
schema = StructType([
    StructField("name", StringType(), True),           # Nome do jogo
    StructField("appid", StringType(), True),          # ID do aplicativo na Steam
    StructField("playtime", FloatType(), True),        # Tempo de jogo em horas
    StructField("img_icon_url", StringType(), True)    # URL do ícone do jogo
])

# Leitura do arquivo JSON com os dados de jogos do usuário
df = spark.read.option("multiline", "true").json("Files/Raw/steam_user.json")

# Explosão do array de jogos em linhas individuais
df = df.select(explode(df.games).alias('games'))

# Seleção das colunas relevantes
df = df.select("games.name", "games.appid", "games.playtime_forever", "games.img_icon_url")

# Conversão do tempo de jogo de minutos para horas
df = df.withColumn("playtime_forever", (col("playtime_forever").cast("float") / 60))

# Renomeação da coluna de tempo de jogo para "playtime"
df = df.withColumnRenamed("playtime_forever", "playtime")

# Criação de um DataFrame com o esquema definido
df = spark.createDataFrame(df.rdd, schema)

# Arredondamento da coluna de tempo de jogo para duas casas decimais
df = df.withColumn('playtime', round(df['playtime'], 2))

# Exibição dos dados transformados
display(df)

# Escrita dos dados transformados em um arquivo CSV
df.write.mode("overwrite").option("header", "true").option("inferSchema", "true").csv("Files/Silver/steam_user.csv")

