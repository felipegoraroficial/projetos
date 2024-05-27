# Databricks notebook source
# Importação das funções necessárias do PySpark e biblioteca requests
from pyspark.sql.functions import explode, regexp_replace, col, when, round
from pyspark.sql.types import StructType, StructField, StringType, FloatType
from pyspark.sql import SparkSession
import requests

# Definição do esquema para os dados de jogos da Steam
schema = StructType([
    StructField("id", StringType(), True),      # ID do jogo
    StructField("link", StringType(), True),    # Link para o jogo
    StructField("name", StringType(), True),    # Nome do jogo
    StructField("img", StringType(), True),     # Imagem do jogo
    StructField("price", FloatType(), True)     # Preço do jogo
])

# Leitura do arquivo JSON com os jogos da Steam
df = spark.read.option("multiline", "true").json("Files/Raw/steam_games.json")

# Explosão do array de jogos em linhas individuais
df = df.select(explode(df.apps).alias('apps'))

# Seleção das colunas relevantes
df = df.select("apps.id", "apps.link", "apps.name", "apps.img", "apps.price")

# Limpeza dos colchetes nas IDs dos jogos
df = df.withColumn("id", regexp_replace(col("id").cast("string"), "[\\[\\]]", ""))

# Remoção do símbolo de dólar e conversão da coluna de preço para float
df = df.withColumn("price", regexp_replace(col("price"), "\\$", "").cast("float"))

# Tratamento de valores nulos na coluna de preço
df = df.withColumn("price", when(col("price").isNull(), 0).otherwise(col("price")))

# Criação de um DataFrame com o esquema definido
df = spark.createDataFrame(df.rdd, schema)

# Inicialização da SparkSession
spark = SparkSession.builder.appName("currencyConversion").getOrCreate()

# Função para obter a taxa de câmbio do dólar para real
def get_exchange_rate():
    response = requests.get("https://economia.awesomeapi.com.br/json/last/USD-BRL")
    data = response.json()
    return float(data['USDBRL']['bid'])

# Obtenção da taxa de câmbio atual
exchange_rate = get_exchange_rate()

# Conversão dos preços para reais usando a taxa de câmbio
df = df.withColumn("price", col("price") * exchange_rate)
df = df.withColumn('price', round(df['price'], 2))

# Exibição dos dados transformados
display(df)

# Escrita dos dados transformados em um arquivo CSV
df.write.mode("overwrite").option("header", "true").option("inferSchema", "true").csv("Files/Silver/steam_games.csv")

