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

countries_df = spark.read.table("movie_silver.countries")

# COMMAND ----------

productions_countries_df = spark.read.table("movie_silver.productions_countries") \
    .filter(f"file_date = '{v_file_date}'")

# COMMAND ----------

productions_companies_df = spark.read.table("movie_silver.productions_companies") \
    .filter(f"file_date = '{v_file_date}'")

# COMMAND ----------

movies_companies_df = spark.read.table("movie_silver.movies_companies") \
    .filter(f"file_date = '{v_file_date}'")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Join "country" y "production_country"

# COMMAND ----------

countries_prod_coun_df = countries_df.join(productions_countries_df,
                                      countries_df.country_id == productions_countries_df.country_id,
                                      "inner") \
                                          .select(countries_df.country_name, productions_countries_df.movie_id)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Join "production_company" y "movie_company"

# COMMAND ----------

production_com_mov_com_df = productions_companies_df.join(movies_companies_df,
                                   productions_companies_df.company_id == movies_companies_df.company_id,
                                   "inner") \
                                       .select(productions_companies_df.company_name, movies_companies_df.movie_id)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Join "movies_df", "countries_prod_coun_df" y "production_com_mov_com_df"

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Filtrar las peliculas donde su fecha sea mayor o igual a 2000

# COMMAND ----------

movies_filter_df = movies_df.filter(movies_df["year_release_date"] >= 2010)

# COMMAND ----------

results_movies_countries_productios_df = movies_filter_df.join(countries_prod_coun_df,
                        movies_filter_df.movie_id == countries_prod_coun_df.movie_id,
                        "inner") \
                            .join(production_com_mov_com_df,
                                  movies_filter_df.movie_id == production_com_mov_com_df.movie_id,
                                  "inner")

# COMMAND ----------

# MAGIC %md
# MAGIC 2. Agregar la columna "create_date"

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

results_df = results_movies_countries_productios_df \
    .select("title", "budget", "revenue", "release_date", "duration_time", "company_name", "country_name") \
    .withColumn("created_date", lit(v_file_date))

# COMMAND ----------

# MAGIC %md
# MAGIC 3. Ordenar por la columna "title" de manera ascendente

# COMMAND ----------

results_order_by_df = results_df.orderBy(results_df.title.asc())

# COMMAND ----------

# MAGIC %md
# MAGIC #### Escribir datos en el DataLake en formato "Parquet"

# COMMAND ----------

#overwrite_partition("movie_gold", "results_country_prod_company", "created_date", v_file_date)

# COMMAND ----------

from delta.tables import DeltaTable

if spark.catalog.tableExists("movie_gold.results_country_prod_company"):

    deltaTable = DeltaTable.forName(spark, "movie_gold.results_country_prod_company")

    deltaTable.alias('tgt') \
    .merge(
        results_order_by_df.alias('src'),
        'tgt.movie_id = src.movie_id AND tgt.created_date = src.created_date'
    ) \
    .whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll() \
    .execute()   

else:

    results_order_by_df.write.mode("overwrite").partitionBy("created_date").format("delta").saveAsTable("movie_gold.results_country_prod_company")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from movie_gold.results_country_prod_company;
