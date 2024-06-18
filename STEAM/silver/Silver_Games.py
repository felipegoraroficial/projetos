# Databricks notebook source
from pyspark.sql.functions import explode, regexp_replace,from_json,col,when,udf,round,to_json,to_date,struct
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, IntegerType, FloatType, LongType, DateType
from bs4 import BeautifulSoup
from pyspark.sql import SparkSession
import requests

# COMMAND ----------

schema = StructType([
    StructField("file_data", DateType(), True),
    StructField("img", StringType(), True),
    StructField("is_free", BooleanType(), True),
    StructField("name", StringType(), True),
    StructField("required_age", IntegerType(), True),
    StructField("short_description", StringType(), True),
    StructField("appid", LongType(), True),
    StructField("type", StringType(), True),
    StructField("website", StringType(), True),
    StructField("minimum", StringType(), True),
    StructField("recommended", StringType(), True),
    StructField("link", StringType(), True),
    StructField("price", FloatType(), True)
])

# COMMAND ----------

spark = SparkSession.builder \
    .appName("Read JSON") \
    .getOrCreate()

# COMMAND ----------

file_path = "dbfs:/bronze/steam/games/games_steam.json"  # Substitua pelo caminho real do seu arquivo JSON
df = spark.read.option("multiline", "true").json(file_path)

# COMMAND ----------

df = df.withColumn("pc_requirements_json", from_json("pc_requirements", schema)) \
       .withColumn("minimum", col("pc_requirements_json.minimum")) \
       .withColumn("recommended", col("pc_requirements_json.recommended")) \
       .drop("pc_requirements_json", "pc_requirements")


# COMMAND ----------

# Explode a coluna "package_groups" para lidar com arrays
df = df.withColumn("package_groups", explode(col("package_groups")))

# Explode a coluna "subs" dentro de "package_groups" para lidar com arrays
df = df.withColumn("subs", explode(col("package_groups.subs")))

# Seleciona todas as colunas do DataFrame original exceto "package_groups" e adiciona a coluna "price_in_cents_with_discount"
columns_to_select = [col for col in df.columns if col not in ["package_groups", "subs"]]
columns_to_select.append("subs.price_in_cents_with_discount")
df = df.select(*columns_to_select)

# COMMAND ----------

df = df.withColumn("required_age", col("required_age").cast("int"))

# COMMAND ----------

string_cols = [f.name for f in df.schema.fields if f.dataType == StringType()]

for col_name in string_cols:
    df = df.withColumn(col_name, when(col(col_name).isNull(), '-').otherwise(col(col_name)))
    df = df.withColumn(col_name, when(col(col_name) == 'nan', '-').otherwise(col(col_name)))

# COMMAND ----------

def extrair_texto_html(html):
    soup = BeautifulSoup(html, 'html.parser')
    texto = soup.get_text()
    return texto

extrair_texto_html_udf = udf(extrair_texto_html, StringType())

df = df.withColumn('minimum', extrair_texto_html_udf(df['minimum']))
df = df.withColumn('recommended', extrair_texto_html_udf(df['recommended']))

# COMMAND ----------

df = df.withColumn('minimum', regexp_replace(df['minimum'], 'Minimum:', ''))
df = df.withColumn('recommended', regexp_replace(df['recommended'], 'Recommended:', ''))

# COMMAND ----------

df = df.withColumn("link", F.concat(F.lit("https://store.steampowered.com/app/"), F.col("steam_appid")))

# COMMAND ----------

df = df.withColumnRenamed('steam_appid', 'appid').withColumnRenamed('header_image', 'img')

# COMMAND ----------

# Função para obter a taxa de câmbio atual do euro para real
def get_exchange_rate():
    response = requests.get('https://api.exchangerate-api.com/v4/latest/EUR')
    data = response.json()
    return data['rates']['BRL']

# Obtenha a taxa de câmbio atual
exchange_rate_eur_to_brl = get_exchange_rate()

# COMMAND ----------

df = df.fillna({'price_in_cents_with_discount': 0}) \
       .withColumn("price", (col("price_in_cents_with_discount") / 100).cast("float")) \
       .withColumn("price_in_brl", round(col("price") * exchange_rate_eur_to_brl, 2)) \
       .drop("price_in_cents_with_discount", "price")

# COMMAND ----------

df = df.withColumn("file_data", to_date(df["file_data"], "yyyy-MM-dd"))

# COMMAND ----------

df = spark.createDataFrame(df.rdd, schema)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

display(df)

# COMMAND ----------

df.write.mode("overwrite").option("header", "true").option("inferSchema", "true").csv("dbfs:/silver/steam/games/steam_games.csv")
