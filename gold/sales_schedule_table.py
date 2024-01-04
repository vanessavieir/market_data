# Databricks notebook source
# 6. *Planilha de Horários de Compra:*
# - Analisar as colunas "Date" e "Time".
# - Identificar os horários de pico de vendas e médias de compras por período do dia.


# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE
# MAGIC OR REPLACE TABLE gold_industria.sales_schedule_table AS (
# MAGIC SELECT 
# MAGIC   date, 
# MAGIC   CASE WHEN hour(to_timestamp(time, 'HH:mm')) BETWEEN 0 AND 5 THEN '00:00 - 05:59'
# MAGIC        WHEN hour(to_timestamp(time, 'HH:mm')) BETWEEN 6 AND 11 THEN '06:00 - 11:59'
# MAGIC        WHEN hour(to_timestamp(time, 'HH:mm')) BETWEEN 12 AND 17 THEN '12:00 - 17:59'
# MAGIC        WHEN hour(to_timestamp(time, 'HH:mm')) BETWEEN 18 AND 23 THEN '18:00 - 23:59'
# MAGIC        ELSE 'Outros horários' END AS time_range,
# MAGIC   MAX(total) AS total_max
# MAGIC FROM 
# MAGIC   bronze_industria.supermarket_sales_v1
# MAGIC GROUP BY 
# MAGIC   date,
# MAGIC   CASE WHEN hour(to_timestamp(time, 'HH:mm')) BETWEEN 0 AND 5 THEN '00:00 - 05:59'
# MAGIC        WHEN hour(to_timestamp(time, 'HH:mm')) BETWEEN 6 AND 11 THEN '06:00 - 11:59'
# MAGIC        WHEN hour(to_timestamp(time, 'HH:mm')) BETWEEN 12 AND 17 THEN '12:00 - 17:59'
# MAGIC        WHEN hour(to_timestamp(time, 'HH:mm')) BETWEEN 18 AND 23 THEN '18:00 - 23:59'
# MAGIC        ELSE 'Outros horários' END
# MAGIC ORDER BY 
# MAGIC   date, 
# MAGIC   time_range
# MAGIC )
