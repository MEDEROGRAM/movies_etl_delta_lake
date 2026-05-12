# Databricks notebook source
# MAGIC %md
# MAGIC #### Leer todos los datos requeridos

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2024-12-30")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

movies_df = spark.read.table("movie_silver.movies") \
    .filter(f"file_date = '{v_file_date}'")

# COMMAND ----------

languages_df = spark.read.table("movie_silver.languages")

# COMMAND ----------

movies_languages_df = spark.read.table("movie_silver.movies_languages") \
    .filter(f"file_date = '{v_file_date}'")

# COMMAND ----------

genres_df = spark.read.table("movie_silver.genres")

# COMMAND ----------

movies_genres_df = spark.read.table("movie_silver.movies_genres") \
    .filter(f"file_date = '{v_file_date}'")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Join "languages" y "movies_languages"

# COMMAND ----------

languages_mov_lan_df = languages_df.join(movies_languages_df,
                                      languages_df.language_id == movies_languages_df.language_id,
                                      "inner") \
                                          .select(languages_df.language_name, movies_languages_df.movie_id)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Join "genres" y "movies_genres"

# COMMAND ----------

genres_mov_gen_df = genres_df.join(movies_genres_df,
                                   genres_df.genre_id == movies_genres_df.genre_id,
                                   "inner") \
                                       .select(genres_df.genre_name, movies_genres_df.movie_id)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Join "movies_df", "languages_mov_lan_df" y "genres_mov_gen_df"

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Filtrar las peliculas donde su fecha sea mayor o igual a 2000

# COMMAND ----------

movie_filter_df = movies_df.filter(movies_df["year_release_date"] >= 2000)

# COMMAND ----------

results_movies_genres_languages_df = movie_filter_df.join(languages_mov_lan_df,
                        movie_filter_df.movie_id == languages_mov_lan_df.movie_id,
                        "inner") \
                            .join(genres_mov_gen_df,
                                  movie_filter_df.movie_id == genres_mov_gen_df.movie_id,
                                  "inner")

# COMMAND ----------

# MAGIC %md
# MAGIC 2. Agregar la columna "create_date"

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

results_df = results_movies_genres_languages_df \
    .select("title", "duration_time", "vote_average", "release_date", "language_name", "genre_name") \
    .withColumn("created_date", lit(v_file_date))

# COMMAND ----------

# MAGIC %md
# MAGIC 3. Ordenar por la columna "release_date" de manera descendente

# COMMAND ----------

results_order_by_df = results_df.orderBy(results_df.release_date.desc())

# COMMAND ----------

# MAGIC %md
# MAGIC #### Escribir datos en el DataLake en formato "Delta"

# COMMAND ----------

#overwrite_partition("movie_gold", "results_movie_genre_language", "created_date", v_file_date)

# COMMAND ----------

from delta.tables import DeltaTable

if spark.catalog.tableExists("movie_gold.results_movie_genre_language"):

    deltaTable = DeltaTable.forName(spark, "movie_gold.results_movie_genre_language")

    deltaTable.alias('tgt') \
    .merge(
        results_order_by_df.alias('src'),
        'tgt.movie_id = src.movie_id'
    ) \
    .whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll() \
    .execute()   

else:

results_order_by_df.write.mode("overwrite").partitionBy("created_date").format("delta").saveAsTable("movie_gold.results_movie_genre_language")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from movie_gold.results_movie_genre_language;
