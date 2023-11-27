# Databricks notebook source
# df_middle.write.saveAsTable('silver_industria.supermarket_sales_v1')

df = spark.sql(" SELECT * FROM bronze_industria.supermarket_sales_v1")

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, DoubleType, IntegerType, DateType, StringType

custom_schema = StructType([
  StructField('Invoice ID', StringType()),
  StructField('Branch', StringType()),
  StructField('City', StringType()),
  StructField('Customer type', StringType()),
  StructField('Gender', StringType()),
  StructField('Age', StringType()),
  StructField('Product line', StringType()),
  StructField('Unit price', DoubleType(), True),
  StructField('Quantity', IntegerType(), True),
  StructField('Tax 5%', DoubleType(), True),
  StructField('Total', DoubleType(), True),
  StructField('Date', DateType()),
  StructField('Time', StringType()),
  StructField('Payment', StringType()),
  StructField('gross income', DoubleType(), True),
  StructField('Rating', DoubleType(), True)
])

df = spark.read.schema(custom_schema) \
            .option("header", True) \
            .option("delimiter", ";") \
            .option("encoding", "latin1") \
            .csv('/FileStore/tables/supermarket_sales_v1.csv')

# COMMAND ----------

df.dtypes

# COMMAND ----------

 pip install unidecode

# COMMAND ----------

from pyspark.sql.functions import col
import unidecode

def normalize_columns(df):
  """Esta função retorna um DataFrame com as colunas normalizadas.
  
  As colunas são convertidas para minúsculas, sem acento e com espaços substituídos por um underscore (_).
  
  :param df: DataFrame a ser normalizado.
  :return: DataFrame normalizado.
  """
  # lista de tuplas representando o esquema das colunas
  schema = df.schema.fields
  
  # converte o nome de cada coluna
  for i, field in enumerate(schema):
    old_name = field.name
    new_name = unidecode.unidecode(old_name).lower().replace(' ', '_')
    df = df.withColumnRenamed(old_name, new_name)
    
  return df

# COMMAND ----------

df= normalize_columns(df)

# COMMAND ----------

# Criando o esquema "silver_industria" se ainda não existir
spark.sql("CREATE SCHEMA IF NOT EXISTS silver_industria")

# COMMAND ----------

df.write.saveAsTable('silver_industria.supermarket_sales_v1')

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT * FROM silver_industria.supermarket_sales_v1

# COMMAND ----------

# Tratamento de dados nulos ou vazios
df = df.na.drop()

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, avg

# Exibir as categorias de produto disponíveis
distinct_categories = df.select("product_line").distinct()
distinct_categories.show(truncate=False)

# Separar os dados com base na coluna "product_line"
category_sales = df.groupBy("product_line").agg(
    sum("total").alias("vendas_totais"),
    sum("quantity").alias("quantidade_produtos"),
    avg("unit_price").alias("media_preco")
)

# Exibir os resultados
print("### Vendas por Categoria de Produto ###")
category_sales.show(truncate=False)

# COMMAND ----------

from pyspark.sql.functions import col, count, when

# Contar linhas nulas em todo o DataFrame
count_nulls_total = df.select([count(when(col(c).isNull(), c)).alias(c) for c in df.columns]).show(truncate=False)


# COMMAND ----------

# Obter todos os valores únicos da coluna "age"
unique_age_values = df.select("age").distinct().rdd.flatMap(lambda x: x).collect()

# Imprimir todos os valores de "age"
for value in unique_age_values:
    print(value)

