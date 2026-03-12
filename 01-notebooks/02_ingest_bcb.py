# Databricks notebook source
catalog_name = "workspace"
bronze_schema = "bronze"
table_name = "bcb_series_bronze"

full_table_name = f"{catalog_name}.{bronze_schema}.{table_name}"

print("Tabela de destino:", full_table_name)

# COMMAND ----------

import requests
import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# COMMAND ----------

series_bcb = {
    "selic": 432,
    "ipca": 433
}

print(series_bcb)

# COMMAND ----------

def extrair_serie_bcb(nome_indicador, codigo_serie, data_inicial, data_final):
    url = (
        f"https://api.bcb.gov.br/dados/serie/bcdata.sgs.{codigo_serie}/dados"
        f"?formato=json&dataInicial={data_inicial}&dataFinal={data_final}"
    )

    response = requests.get(url, timeout=30)
    response.raise_for_status()

    dados = response.json()

    for linha in dados:
        linha["indicador"] = nome_indicador
        linha["codigo_serie"] = str(codigo_serie)

    return dados

# COMMAND ----------

dados_selic = extrair_serie_bcb(
    "selic",
    432,
    "01/01/2015",
    "31/12/2024"
)

print("Quantidade de registros:", len(dados_selic))
print("Primeiro registro:", dados_selic[0])
print("Último registro:", dados_selic[-1])

# COMMAND ----------

dados_bcb = []

for nome_indicador, codigo_serie in series_bcb.items():
    dados = extrair_serie_bcb(
        nome_indicador,
        codigo_serie,
        "01/01/2015",
        "31/12/2024"
    )
    dados_bcb.extend(dados)

print("Total de registros coletados:", len(dados_bcb))

# COMMAND ----------

df_pd = pd.DataFrame(dados_bcb)

print("Quantidade de linhas no pandas:", len(df_pd))
print("Colunas:", df_pd.columns.tolist())

df_pd.head()

# COMMAND ----------

df_pd["data_referencia"] = pd.to_datetime(df_pd["data"], format="%d/%m/%Y", errors="coerce")

df_pd["valor"] = (
    df_pd["valor"]
    .astype(str)
    .str.replace(",", ".", regex=False)
)

df_pd["valor"] = pd.to_numeric(df_pd["valor"], errors="coerce")

df_pd["data_ingestao"] = pd.Timestamp.now()

df_pd = df_pd[["indicador", "codigo_serie", "data_referencia", "valor", "data_ingestao"]]

print("Tipos de dados após padronização:")
print(df_pd.dtypes)

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
    codigo_serie,
    MIN(data_referencia) AS data_min,
    MAX(data_referencia) AS data_max,
    COUNT(*) AS total_registros
FROM {full_table_name}
GROUP BY indicador, codigo_serie
ORDER BY indicador
""").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC # Bronze BCB
# MAGIC
# MAGIC Este notebook realiza a ingestão de séries temporais do Banco Central do Brasil via API SGS.
# MAGIC
# MAGIC ## Indicadores incluídos nesta etapa
# MAGIC - Selic (código 432)
# MAGIC - IPCA (código 433)
# MAGIC
# MAGIC ## Intervalo coletado
# MAGIC - 01/01/2015 a 31/12/2024
# MAGIC
# MAGIC ## Tabela de destino
# MAGIC - workspace.bronze.bcb_series_bronze