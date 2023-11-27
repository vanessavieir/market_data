# Databricks notebook source
# 2. *Planilha de Desempenho do Atendimento ao Cliente:*

# - Analisar a coluna "Rating" para entender a satisfação do cliente.

# - Calcular a média de classificação global e por filial.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE
# MAGIC OR REPLACE TABLE gold_industria.branch_and_global_rating AS (
# MAGIC   SELECT
# MAGIC     branch,
# MAGIC     AVG(rating) AS branch_rating,
# MAGIC     global_rating
# MAGIC   FROM
# MAGIC     (
# MAGIC       SELECT
# MAGIC         branch,
# MAGIC         rating,
# MAGIC         AVG(rating) OVER () AS global_rating
# MAGIC       FROM
# MAGIC         bronze_industria.supermarket_sales_v1
# MAGIC     )
# MAGIC   GROUP BY
# MAGIC     branch,
# MAGIC     global_rating
# MAGIC   ORDER BY
# MAGIC     branch
# MAGIC )
