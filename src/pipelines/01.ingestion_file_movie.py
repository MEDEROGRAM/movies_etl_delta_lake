# Databricks notebook source
# MAGIC %md
# MAGIC #### Ingestion del archivo "movie.csv"

# COMMAND ----------

dbutils.widgets.text("p_environment", "")
v_environment = dbutils.widgets.get("p_environment")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2024-12-30")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Paso 1 - Leer el archivo CSV usando "DataFrameReader" de Spark

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType, DateType

# COMMAND ----------

movie_schema = StructType( fields=[
    StructField("movieId", IntegerType(), False),
    StructField("title", StringType(), True),
    StructField("budget", DoubleType(), True),
    StructField("homePage", StringType(), True),
    StructField("overview", StringType(), True),
    StructField("popularity", StringType(), True),
    StructField("yearReleaseDate", IntegerType(), True),
    StructField("releaseDate", DateType(), True),
    StructField("revenue", DoubleType(), True),
    StructField("durationTime", IntegerType(), True),
    StructField("movieStatus", StringType(), True),
    StructField("tagline", StringType(), True),
    StructField("voteAverage", DoubleType(), True),
    StructField("voteCount", IntegerType(), True), 
])

# COMMAND ----------

movie_df = spark.read \
    .option("header", True) \
    .schema(movie_schema) \
    .csv(f"{bronze_folder_path}/{v_file_date}/movie.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Paso 2- Seleccionar sólo las columnas "Requeridas"

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

movies_selected_df = movie_df.select(col("movieId"), col("title"), col("budget"), col("popularity"), col("yearReleaseDate"), col("releaseDate"), col("revenue"), col("durationTime"), col("voteAverage"), col("voteCount"))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Paso 3 - Cambiar el nombre de las columnas según lo "Requerido"

# COMMAND ----------

# --- MIS APUNTES: Forma eficiente con diccionario (Plural) ---
# mapeo_columnas = {
#     "movieId": "movie_id",
#     "yearReleaseDate": "year_release_date"
# }
# movies_renamed_df = movies_selected_df.withColumnsRenamed(mapeo_columnas)
# -----------------------------------------------------------

# COMMAND ----------

movies_renamed_df = movies_selected_df \
    .withColumnRenamed("movieId", "movie_id") \
    .withColumnRenamed("yearReleaseDate", "year_release_date") \
    .withColumnRenamed("releaseDate", "release_date") \
    .withColumnRenamed("durationTime", "duration_time") \
    .withColumnRenamed("voteAverage", "vote_average") \
    .withColumnRenamed("voteCount", "vote_count")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Paso 4 - Agregar la columna "Ingestion Date" al DataFrame

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

movies_final_df = add_insgestion_date(movies_renamed_df) \
    .withColumn("environment", lit(v_environment)) \
    .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Paso 5 - Escribir datos en el datalake en formato "Delta"

# COMMAND ----------

#overwrite_partition("movie_silver", "movies", "file_date", v_file_date)

# COMMAND ----------

merge_condition = 'tgt.movie_id = src.movie_id AND tgt.file_date = src.file_date'
merge_delta_lake(movies_final_df, "movie_silver", "movies", merge_condition, "file_date")

# COMMAND ----------

# MAGIC %sql
# MAGIC select file_date, count(1)
# MAGIC from movie_silver.movies
# MAGIC group by file_date;

# COMMAND ----------

# MAGIC %sql
# MAGIC describe extended movie_silver.movies;

# COMMAND ----------

dbutils.notebook.exit("Success")