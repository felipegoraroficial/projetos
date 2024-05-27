# Databricks notebook source
# Importação das funções necessárias do PySpark e BeautifulSoup
from pyspark.sql.functions import explode, regexp_replace, from_json, col, when, udf
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, IntegerType
from bs4 import BeautifulSoup

# Definição do esquema para os requisitos de PC dos jogos
schema = StructType([
    StructField("steam_appid", StringType(), True),           # ID do jogo na Steam
    StructField("name", StringType(), True),                  # Nome do jogo
    StructField("type", StringType(), True),                  # Tipo de jogo
    StructField("is_free", BooleanType(), True),              # Indica se o jogo é gratuito
    StructField("required_age", IntegerType(), True),         # Idade mínima recomendada
    StructField("controller_support", StringType(), True),    # Suporte a controle
    StructField("dlc", StringType(), True),                   # DLCs disponíveis
    StructField("website", StringType(), True),               # Website do jogo
    StructField("minimum", StringType(), True),               # Requisitos mínimos de sistema
    StructField("recommended", StringType(), True)            # Requisitos recomendados de sistema
])

# Leitura do arquivo JSON com os detalhes dos jogos da Steam
df = spark.read.option("multiline", "true").json("Files/Raw/steam_details.json")

# Seleção das colunas relevantes
df = df.select(
    "data.steam_appid",           # ID do jogo na Steam
    "data.name",                  # Nome do jogo
    "data.type",                  # Tipo de jogo
    "data.is_free",               # Indica se o jogo é gratuito
    "data.required_age",          # Idade mínima recomendada
    "data.controller_support",    # Suporte a controle
    "data.dlc",                   # DLCs disponíveis
    "data.website",               # Website do jogo
    "data.pc_requirements"        # Requisitos de sistema
)

# Transformação da coluna de requisitos de sistema em JSON e seleção das subcolunas
df = df.withColumn("pc_requirements_json", from_json("pc_requirements", schema)) \
       .withColumn("minimum", col("pc_requirements_json.minimum")) \
       .withColumn("recommended", col("pc_requirements_json.recommended")) \
       .drop("pc_requirements_json", "pc_requirements")

# Conversão da coluna "required_age" para o tipo inteiro
df = df.withColumn("required_age", col("required_age").cast("int"))

# Criação de um DataFrame com o esquema definido
df = spark.createDataFrame(df.rdd, schema)

# Limpeza dos colchetes nas colunas de DLCs
df = df.withColumn('dlc', regexp_replace(col('dlc'), "\[", ""))
df = df.withColumn('dlc', regexp_replace(col('dlc'), "\]", ""))

# Seleção das colunas de tipo String
string_cols = [f.name for f in df.schema.fields if f.dataType == StringType()]

# Tratamento de valores nulos e 'nan' nas colunas de tipo String
for col_name in string_cols:
    df = df.withColumn(col_name, when(col(col_name).isNull(), '-').otherwise(col(col_name)))
    df = df.withColumn(col_name, when(col(col_name) == 'nan', '-').otherwise(col(col_name)))

# Função para extrair texto de campos HTML usando BeautifulSoup
def extrair_texto_html(html):
    soup = BeautifulSoup(html, 'html.parser')
    texto = soup.get_text()
    return texto

# Criação de uma UDF (User Defined Function) para a função de extração de texto HTML
extrair_texto_html_udf = udf(extrair_texto_html, StringType())

# Aplicação da UDF para extrair texto das colunas de requisitos mínimos e recomendados
df = df.withColumn('minimum', extrair_texto_html_udf(df['minimum']))
df = df.withColumn('recommended', extrair_texto_html_udf(df['recommended']))

# Remoção dos prefixos 'Minimum:' e 'Recommended:' dos respectivos campos
df = df.withColumn('minimum', regexp_replace(df['minimum'], 'Minimum:', ''))
df = df.withColumn('recommended', regexp_replace(df['recommended'], 'Recommended:', ''))

# Exibição dos dados transformados
display(df)

# Escrita dos dados transformados em um arquivo CSV
df.write.mode("overwrite").option("header", "true").option("inferSchema", "true").csv("Files/Silver/steam_details.csv")

