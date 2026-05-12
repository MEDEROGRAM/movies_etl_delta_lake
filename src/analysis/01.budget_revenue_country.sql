-- Databricks notebook source
-- MAGIC %md
-- MAGIC ### Analisis sobre presupuesto e ingresos por pais

-- COMMAND ----------

use movie_gold;

-- COMMAND ----------

select *
from results_movie;

-- COMMAND ----------

select country_name,
       count(country_name) as total_movie,
       sum(budget) as total_budget,
       sum(revenue) as total_revenue
from results_movie
group by country_name
order by total_revenue desc;

-- COMMAND ----------

select country_name,
       count(country_name) as total_movie,
       sum(budget) as total_budget,
       cast(avg(budget) as decimal(18,2)) as avg_budget,
       sum(revenue) as total_revenue,
       cast(avg(revenue) as decimal(18,2)) as avg_revenue
from results_movie
group by country_name
order by total_revenue desc;

-- COMMAND ----------

select country_name,
       count(country_name) as total_movie,
       sum(budget) as total_budget,
       cast(avg(budget) as decimal(18,2)) as avg_budget,
       sum(revenue) as total_revenue,
       cast(avg(revenue) as decimal(18,2)) as avg_revenue
from results_movie
where year_release_date between 2010 and 2015
group by country_name
order by total_revenue desc;