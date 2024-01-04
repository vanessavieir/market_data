# Databricks notebook source
# 9. *Planilha de Análise de Margem de Lucro:*

# - Calcular a margem de lucro para cada transação.

# # - Analisar a margem de lucro média por categoria de produto.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE gold_industria.product_profit_analysis AS (
# MAGIC   SELECT
# MAGIC     product_line,
# MAGIC     AVG(total - gross_income) AS avg_profit
# MAGIC   FROM
# MAGIC     bronze_industria.supermarket_sales_v1
# MAGIC   GROUP BY
# MAGIC     product_line
# MAGIC )
