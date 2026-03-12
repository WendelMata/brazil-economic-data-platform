# Databricks notebook source
catalog_name = "workspace"
bronze_schema = "bronze"
table_name = "ibge_pnad_desocupacao_bronze"

full_table_name = f"{catalog_name}.{bronze_schema}.{table_name}"

print("Tabela de destino:", full_table_name)

# COMMAND ----------

import requests
import pandas as pd
from pyspark.sql import functions as F

# COMMAND ----------

url = "https://apisidra.ibge.gov.br/values/t/6381/n1/all/v/4099/p/201201-202412"

response = requests.get(url, timeout=60)

print("Status code:", response.status_code)
print("Primeiros 300 caracteres da resposta:")
print(response.text[:300])

# COMMAND ----------

dados_ibge = response.json()

print("Quantidade de linhas retornadas:", len(dados_ibge))
print("Primeira linha:")
print(dados_ibge[0])
print("Segunda linha:")
print(dados_ibge[1])

# COMMAND ----------

df_pd = pd.DataFrame(dados_ibge[1:])

print("Quantidade de linhas:", len(df_pd))
print("Colunas:", df_pd.columns.tolist())

df_pd.head()

# COMMAND ----------

df_pd["indicador"] = "taxa_desocupacao"
df_pd["fonte"] = "IBGE_SIDRA"
df_pd["codigo_tabela"] = "6381"
df_pd["codigo_variavel"] = "4099"

df_pd["valor"] = (
    df_pd["V"]
    .astype(str)
    .str.replace(",", ".", regex=False)
)

df_pd["valor"] = pd.to_numeric(df_pd["valor"], errors="coerce")

df_pd["data_ingestao"] = pd.Timestamp.now()

df_pd = df_pd[["indicador", "fonte", "codigo_tabela", "codigo_variavel", "D3C", "D3N", "V", "valor", "data_ingestao"]]

df_pd.head()

# COMMAND ----------

df_pd = pd.DataFrame(dados_ibge[1:])

print("Colunas atuais do df_pd:")
print(df_pd.columns.tolist())

df_pd.head()

# COMMAND ----------

df_pd["indicador"] = "taxa_desocupacao"
df_pd["fonte"] = "IBGE_SIDRA"
df_pd["codigo_tabela"] = "6381"
df_pd["codigo_variavel"] = "4099"

df_pd["valor"] = (
    df_pd["V"]
    .astype(str)
    .str.replace(",", ".", regex=False)
)

df_pd["valor"] = pd.to_numeric(df_pd["valor"], errors="coerce")

df_pd["data_ingestao"] = pd.Timestamp.now()

print("Colunas após transformação:")
print(df_pd.columns.tolist())

# COMMAND ----------

df_pd["D3C"] = df_pd["D3C"].astype(str)

df_pd["ano"] = df_pd["D3C"].str[:4]
df_pd["mes"] = df_pd["D3C"].str[4:6]

df_pd["data_referencia"] = pd.to_datetime(
    df_pd["ano"] + "-" + df_pd["mes"] + "-01",
    errors="coerce"
)

print(df_pd[["D3C", "D3N", "data_referencia"]].head())

# COMMAND ----------

df_pd = df_pd[[
    "indicador",
    "fonte",
    "codigo_tabela",
    "codigo_variavel",
    "D3C",
    "D3N",
    "data_referencia",
    "valor",
    "data_ingestao"
]]

print("Colunas finais:")
print(df_pd.columns.tolist())

df_pd.head()

# COMMAND ----------

print("Valores nulos por coluna:")
print(df_pd.isnull().sum())

# COMMAND ----------

df_spark = spark.createDataFrame(df_pd)

print("Schema do DataFrame Spark:")
df_spark.printSchema()

# COMMAND ----------

df_spark.show(10, truncate=False)

# COMMAND ----------

df_spark.write.format("delta").mode("overwrite").saveAsTable(full_table_name)

print(f"Tabela {full_table_name} gravada com sucesso.")

# COMMAND ----------

spark.sql(f"""
SELECT
    indicador,
    MIN(data_referencia) AS data_min,
    MAX(data_referencia) AS data_max,
    COUNT(*) AS total_registros
FROM {full_table_name}
GROUP BY indicador
""").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC # Bronze IBGE
# MAGIC
# MAGIC Este notebook realiza a ingestão da taxa de desocupação a partir da API SIDRA do IBGE.
# MAGIC
# MAGIC ## Fonte
# MAGIC - SIDRA / IBGE
# MAGIC
# MAGIC ## Tabela consultada
# MAGIC - 6381
# MAGIC
# MAGIC ## Variável
# MAGIC - 4099
# MAGIC
# MAGIC ## Indicador
# MAGIC - Taxa de desocupação
# MAGIC
# MAGIC ## Tabela de destino
# MAGIC - workspace.bronze.ibge_pnad_desocupacao_bronze