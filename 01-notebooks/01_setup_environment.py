# Databricks notebook source
spark.sql("SHOW CATALOGS").show(truncate=False)

# COMMAND ----------

print("Ambiente conectado com sucesso.")

# COMMAND ----------

spark.sql("USE CATALOG workspace")

spark.sql("CREATE SCHEMA IF NOT EXISTS bronze")
spark.sql("CREATE SCHEMA IF NOT EXISTS silver")
spark.sql("CREATE SCHEMA IF NOT EXISTS gold")

print("Schemas bronze, silver e gold criados com sucesso no catálogo workspace.")

# COMMAND ----------

spark.sql("SHOW SCHEMAS IN workspace").show(truncate=False)

# COMMAND ----------

from pyspark.sql import Row

dados = [
    Row(indicador="selic", data_referencia="2025-01-01", valor=12.25),
    Row(indicador="ipca", data_referencia="2025-01-01", valor=4.83)
]

df = spark.createDataFrame(dados)

df.write.format("delta").mode("overwrite").saveAsTable("workspace.bronze.tabela_teste")

print("Tabela workspace.bronze.tabela_teste criada com sucesso.")

# COMMAND ----------

spark.sql("SELECT * FROM workspace.bronze.tabela_teste").show()