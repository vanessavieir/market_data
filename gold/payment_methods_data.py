# Databricks notebook source
# 4. *Planilha de Meios de Pagamento:*

# - Analisar a coluna "Payment" para entender a distribuição de pagamentos.

# - Calcular as vendas totais e médias para cada método de pagamento.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE gold_industria.payment_methods_data AS (
# MAGIC   SELECT
# MAGIC     payment,
# MAGIC     SUM(total) AS total_payment,
# MAGIC     AVG(total) AS avg_payment
# MAGIC   FROM
# MAGIC     bronze_industria.supermarket_sales_v1
# MAGIC   GROUP BY
# MAGIC     payment
# MAGIC   ORDER BY
# MAGIC     payment
# MAGIC )
