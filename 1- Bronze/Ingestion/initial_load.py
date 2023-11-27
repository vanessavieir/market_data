# Databricks notebook source
# dbutils.fs.ls("/FileStore/tables")

# COMMAND ----------

df = spark.read.option("header", True) \
             .option("delimiter", ";") \
             .option("encoding", "latin1") \
             .csv('/FileStore/tables/supermarket_sales_v1.csv')
 
# df.write.mode("overwrite").saveAsTable('bronze_leochalhoub_nttdata.supermarket_sales_v1')

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT * FROM bronze_industria.supermarket_sales_v1
