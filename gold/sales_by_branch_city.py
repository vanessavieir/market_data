# Databricks notebook source
# 7. *Planilha de Desempenho por Filial e Cidade:*

# - Analisar as colunas "Branch" e "City".

# - Calcular as vendas totais, a receita bruta e o lucro para cada filial e cidade.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE
# MAGIC OR REPLACE TABLE gold_industria.sales_by_branch_city AS (
# MAGIC   SELECT
# MAGIC     branch,
# MAGIC     city,
# MAGIC     SUM(total) AS total_sales,
# MAGIC     SUM(gross_income) as gross_income,
# MAGIC     SUM(total - gross_income) as profit
# MAGIC   FROM
# MAGIC     bronze_industria.supermarket_sales_v1
# MAGIC   GROUP BY
# MAGIC     branch,
# MAGIC     city
# MAGIC   ORDER BY
# MAGIC     branch,
# MAGIC     city
# MAGIC )
