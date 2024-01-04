# Databricks notebook source
# 3. *Planilha de Vendas Mensais:*

# - Extrair o mês e ano da coluna "Date".

# - Agregar as vendas totais, a receita bruta e o lucro por mês.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE
# MAGIC OR REPLACE TABLE gold_industria.monthly_sales_summary AS (
# MAGIC   SELECT
# MAGIC     YEAR(date) AS year,
# MAGIC     MONTH(date) AS month,
# MAGIC     SUM(total) AS total_sales,
# MAGIC     SUM(gross_income) AS gross_income,
# MAGIC     SUM(total - gross_income) as profit
# MAGIC   FROM
# MAGIC     bronze_industria.supermarket_sales_v1
# MAGIC   GROUP BY
# MAGIC     YEAR(date),
# MAGIC     MONTH(date)
# MAGIC   ORDER BY
# MAGIC     year,
# MAGIC     month
# MAGIC )
