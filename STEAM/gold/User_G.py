# Databricks notebook source
# Importação das funções necessárias do PySpark
from pyspark.sql.functions import when, col
from pyspark.sql.types import StringType

# Leitura do arquivo CSV com os dados de jogos do usuário da Steam
df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("Files/Silver/steam_user.csv")

# Leitura do arquivo CSV com os detalhes dos jogos da Steam
df1 = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("Files/Silver/steam_details.csv")

# Leitura do arquivo CSV com os dados dos jogos da Steam
df2 = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("Files/Silver/steam_games.csv")

# Remoção da coluna 'img_icon_url'
df = df.drop('img_icon_url')

# Junção dos dados do usuário com os detalhes dos jogos da Steam
df = df.join(df1, df.appid == df1.steam_appid, 'left').select(df["*"], df1["website"], df1["minimum"], df1["recommended"])

# Junção dos dados resultantes com os dados dos jogos da Steam
df = df.join(df2, df.appid == df2.id, 'left').select(df["*"], df2["img"], df2["price"], df2["link"])

# Seleção das colunas de tipo String
string_cols = [f.name for f in df.schema.fields if f.dataType == StringType()]

# Tratamento de valores nulos e 'nan' nas colunas de tipo String
for col_name in string_cols:
    df = df.withColumn(col_name, when(col(col_name).isNull(), '-').otherwise(col(col_name)))
    df = df.withColumn(col_name, when(col(col_name) == 'nan', '-').otherwise(col(col_name)))

# Tratamento de valores nulos na coluna de preço
df = df.withColumn('price', when(df['price'].isNull(), 0).otherwise(df['price']))

# Exibição dos dados transformados
display(df)

# Escrita dos dados transformados em uma tabela Delta
df.write.format("delta").mode("overwrite").saveAsTable("steam_user")

