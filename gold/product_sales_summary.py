# Databricks notebook source
# 8. *Planilha de Produtos Mais Vendidos:*

# - Identificar os produtos mais vendidos com base na quantidade e receita.

# # - Calcular a participação percentual de cada produto nas vendas totais.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE gold_industria.product_sales_summary AS (
# MAGIC   SELECT
# MAGIC     product_line,
# MAGIC     SUM(quantity) AS total_quantity,
# MAGIC     SUM(total) AS total_product_line,
# MAGIC     total_sales,
# MAGIC     SUM(total) / total_sales AS percent_part
# MAGIC   FROM
# MAGIC     (
# MAGIC       SELECT
# MAGIC         *,
# MAGIC         SUM(total) OVER() AS total_sales
# MAGIC       FROM
# MAGIC         bronze_industria.supermarket_sales_v1
# MAGIC     )
# MAGIC   GROUP BY
# MAGIC     product_line,
# MAGIC     total_sales
# MAGIC   ORDER BY
# MAGIC     total_sales DESC
# MAGIC )
