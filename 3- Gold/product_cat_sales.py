# Databricks notebook source
# 1. *Planilha de Vendas por Categoria de Produto:*

# - Separar os dados com base na coluna "Product line".

# - Calcular as vendas totais, a quantidade de produtos vendidos e a média de preço por categoria.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE gold_industria.product_cat_sales AS (
# MAGIC SELECT
# MAGIC   product_line,
# MAGIC   SUM(total) AS total_sales,
# MAGIC   SUM(quantity) AS total_quantity,
# MAGIC   AVG(unit_price) AS avg_price
# MAGIC FROM
# MAGIC   bronze_industria.supermarket_sales_v1
# MAGIC GROUP BY
# MAGIC   product_line
# MAGIC ORDER BY product_line
# MAGIC )
