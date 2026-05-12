-- Databricks notebook source
-- MAGIC %md
-- MAGIC #### Crear la Tabla "results_movie" en la capa "gold"

-- COMMAND ----------

use movie_gold;

-- COMMAND ----------

create table movie_gold.results_movie
using delta
as
select M.year_release_date, C.country_name, PCO.company_name, M.budget, M.revenue
from movies M
inner join productions_countries PC on M.movie_id = PC.movie_id
inner join countries C on PC.country_id = C.country_id
inner join movies_companies MC on M.movie_id = MC.movie_id
inner join productions_companies PCO on MC.company_id = PCO.company_id