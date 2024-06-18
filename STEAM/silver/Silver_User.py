# Databricks notebook source
from pyspark.sql.functions import explode, regexp_replace,from_json,col,when,udf,round,to_json,expr,to_date
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, IntegerType, FloatType, DateType, LongType
from bs4 import BeautifulSoup
from pyspark.sql import SparkSession

schema = StructType([
    StructField("appid", LongType(), True),
    StructField("file_data", DateType(), True),
    StructField("img", StringType(), True),
    StructField("name", StringType(), True),
    StructField("playtime", FloatType(), True),
    StructField("short_description", StringType(), True),
    StructField("website", StringType(), True),
    StructField("minimum", StringType(), True),
    StructField("recommended", StringType(), True),
    StructField("link", StringType(), True),
])

# COMMAND ----------

spark = SparkSession.builder \
    .appName("Read JSON") \
    .getOrCreate()

# COMMAND ----------

df = spark.read.option("multiline", "true").json("dbfs:/bronze/steam/user/user_steam.json")

# COMMAND ----------

df = df.select("appid", "file_data", "header_image", "name", "pc_requirements", "playtime_forever", "short_description", "website")

# COMMAND ----------

df = df.withColumnRenamed("playtime_forever", "playtime")\
       .withColumnRenamed("header_image", "img")


# COMMAND ----------

df = df.withColumn("playtime", round((col("playtime").cast("float") / 60), 2))

# COMMAND ----------

df = df.withColumn("pc_requirements_json", from_json(to_json(col("pc_requirements")), schema)) \
       .withColumn("minimum", col("pc_requirements_json.minimum")) \
       .withColumn("recommended", col("pc_requirements_json.recommended")) \
       .drop("pc_requirements_json", "pc_requirements")

# COMMAND ----------

def extrair_texto_html(html):
    if html is None:
        return ""
    soup = BeautifulSoup(html, 'html.parser')
    texto = soup.get_text()
    return texto

extrair_texto_html_udf = udf(extrair_texto_html, StringType())

df = df.withColumn('minimum', when((df['minimum'].isNull()) | (df['minimum'] == ""), "-").otherwise(extrair_texto_html_udf(df['minimum'])))
df = df.withColumn('recommended', when((df['recommended'].isNull()) | (df['recommended'] == ""), "-").otherwise(extrair_texto_html_udf(df['recommended'])))

# COMMAND ----------

df = df.withColumn('minimum', expr("regexp_replace(minimum, 'Minimum:', '')"))
df = df.withColumn('minimum', expr("regexp_replace(minimum, 'Minimum', '')"))
df = df.withColumn('recommended', expr("regexp_replace(recommended, 'Recommended:', '')"))
df = df.withColumn('recommended', expr("regexp_replace(recommended, 'Recommended', '')"))

# COMMAND ----------

df = df.withColumn("link", F.concat(F.lit("https://store.steampowered.com/app/"), F.col("appid")))

# COMMAND ----------

df = df.withColumn("file_data", to_date(df["file_data"], "yyyy-MM-dd"))

# COMMAND ----------

df = spark.createDataFrame(df.rdd, schema)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

string_cols = [f.name for f in df.schema.fields if f.dataType == StringType()]

for col_name in string_cols:
    df = df.withColumn(col_name, when(col(col_name).isNull(), '-').otherwise(col(col_name)))
    df = df.withColumn(col_name, when(col(col_name) == 'nan', '-').otherwise(col(col_name)))

# COMMAND ----------

display(df)

# COMMAND ----------

df.write.mode("overwrite").option("header", "true").option("inferSchema", "true").csv("dbfs:/silver/steam/user/steam_user.csv")
