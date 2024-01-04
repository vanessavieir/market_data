# Databricks notebook source
# 5. *Planilha de Demografia do Cliente:*

# - Analisar as colunas "Customer type", "Gender" e "Age".

# - Calcular a distribuição de clientes por tipo, gênero e faixa etária.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE gold_industria.customer_gender_age_count AS (
# MAGIC   SELECT
# MAGIC     customer_type,
# MAGIC     gender,
# MAGIC     age,
# MAGIC     COUNT(*) AS count_client
# MAGIC   FROM
# MAGIC     bronze_industria.supermarket_sales_v1
# MAGIC   GROUP BY
# MAGIC     customer_type,
# MAGIC     gender,
# MAGIC     age
# MAGIC   ORDER BY
# MAGIC     count_client DESC
# MAGIC )
