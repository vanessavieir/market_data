# Databricks notebook source
# dbutils.fs.ls("/FileStore/tables")

# COMMAND ----------

from pyspark.sql.functions import to_date, when, col,udf
from pyspark.sql.types import DateType
from datetime import datetime

def parse_date(date_str):
    try:            
        return datetime.strptime(date_str, "%Y/%m/%d")     
    except ValueError:            
        return datetime.strptime(date_str, "%m/%d/%Y") 
    
parse_date_udf = udf(parse_date, DateType())

# COMMAND ----------

df = df.withColumn("date", parse_date_udf(col("date")))
df.write.mode("overwrite").saveAsTable('bronze_industria.supermarket_sales_v1')

# COMMAND ----------

from pyspark.sql.types import *

schema = StructType(
    [
        StructField("invoice_id", StringType(), True),
        StructField("branch", StringType(), True),
        StructField("city", StringType(), True),
        StructField("customer_type", StringType(), True),
        StructField("gender", StringType(), True),
        StructField("age", StringType(), True),
        StructField("product_line", StringType(), True),
        StructField("unit_price", DoubleType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("tax_5%", DoubleType(), True),
        StructField("total", DoubleType(), True),
        StructField("date", StringType(), True),
        StructField("time", StringType(), True),
        StructField("payment", StringType(), True),
        StructField("gross_income", DoubleType(), True),
        StructField("rating", DoubleType(), True),
    ]
)
df = (
    spark.read.option("header", True)
    .option("delimiter", ";")
    .option("encoding", "latin1")
    .schema(schema)
    .csv("/FileStore/tables/supermarket_sales_v1.csv")
)
