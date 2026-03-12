# Databricks notebook source
catalog_name = "workspace"

bronze_bcb_table = f"{catalog_name}.bronze.bcb_series_bronze"
bronze_ibge_table = f"{catalog_name}.bronze.ibge_pnad_desocupacao_bronze"

silver_bcb_table = f"{catalog_name}.silver.bcb_series_silver"
silver_ibge_table = f"{catalog_name}.silver.ibge_desocupacao_silver"
silver_unificada_table = f"{catalog_name}.silver.indicadores_economicos_silver"

print("Bronze BCB:", bronze_bcb_table)
print("Bronze IBGE:", bronze_ibge_table)
print("Silver BCB:", silver_bcb_table)
print("Silver IBGE:", silver_ibge_table)
print("Silver Unificada:", silver_unificada_table)

# COMMAND ----------

df_bcb = spark.table(bronze_bcb_table)
df_ibge = spark.table(bronze_ibge_table)

print("Leitura das bronzes realizada com sucesso.")

# COMMAND ----------

print("Schema BCB:")
df_bcb.printSchema()

print("Schema IBGE:")
df_ibge.printSchema()

# COMMAND ----------

print("Amostra BCB:")
df_bcb.show(5, truncate=False)

print("Amostra IBGE:")
df_ibge.show(5, truncate=False)

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

df_bcb_silver = (
    df_bcb
    .withColumn("fonte", F.lit("BCB_SGS"))
    .withColumn("codigo_origem", F.col("codigo_serie"))
    .withColumn("descricao_origem", F.col("indicador"))
    .withColumn("ano", F.year("data_referencia"))
    .withColumn("mes", F.month("data_referencia"))
    .withColumn("trimestre", F.quarter("data_referencia"))
    .withColumn("ano_mes", F.date_format("data_referencia", "yyyy-MM"))
    .select(
        "indicador",
        "fonte",
        "codigo_origem",
        "descricao_origem",
        "data_referencia",
        "ano",
        "mes",
        "trimestre",
        "ano_mes",
        "valor",
        "data_ingestao"
    )
)

print("Silver BCB criada em memória.")

# COMMAND ----------

df_bcb_silver.printSchema()
df_bcb_silver.show(5, truncate=False)

# COMMAND ----------

df_bcb_silver.write.format("delta").mode("overwrite").saveAsTable(silver_bcb_table)

print(f"Tabela {silver_bcb_table} gravada com sucesso.")

# COMMAND ----------

df_ibge_silver = (
    df_ibge
    .withColumn("codigo_origem", F.col("codigo_variavel"))
    .withColumn("descricao_origem", F.col("D3N"))
    .withColumn("ano", F.year("data_referencia"))
    .withColumn("mes", F.month("data_referencia"))
    .withColumn("trimestre", F.quarter("data_referencia"))
    .withColumn("ano_mes", F.date_format("data_referencia", "yyyy-MM"))
    .select(
        "indicador",
        "fonte",
        "codigo_origem",
        "descricao_origem",
        "data_referencia",
        "ano",
        "mes",
        "trimestre",
        "ano_mes",
        "valor",
        "data_ingestao"
    )
)

print("Silver IBGE criada em memória.")

# COMMAND ----------

df_ibge_silver.printSchema()
df_ibge_silver.show(5, truncate=False)

# COMMAND ----------

df_ibge_silver.write.format("delta").mode("overwrite").saveAsTable(silver_ibge_table)

print(f"Tabela {silver_ibge_table} gravada com sucesso.")

# COMMAND ----------

df_silver_unificada = df_bcb_silver.unionByName(df_ibge_silver)

print("Silver unificada criada em memória.")

# COMMAND ----------

df_silver_unificada.printSchema()
df_silver_unificada.show(10, truncate=False)

# COMMAND ----------

df_silver_unificada.write.format("delta").mode("overwrite").saveAsTable(silver_unificada_table)

print(f"Tabela {silver_unificada_table} gravada com sucesso.")

# COMMAND ----------

spark.sql(f"""
SELECT
    indicador,
    fonte,
    MIN(data_referencia) AS data_min,
    MAX(data_referencia) AS data_max,
    COUNT(*) AS total_registros
FROM {silver_unificada_table}
GROUP BY indicador, fonte
ORDER BY indicador, fonte
""").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC # Bronze to Silver
# MAGIC
# MAGIC Este notebook realiza a transformação das tabelas Bronze em tabelas Silver padronizadas.
# MAGIC
# MAGIC ## Entradas
# MAGIC - workspace.bronze.bcb_series_bronze
# MAGIC - workspace.bronze.ibge_pnad_desocupacao_bronze
# MAGIC
# MAGIC ## Saídas
# MAGIC - workspace.silver.bcb_series_silver
# MAGIC - workspace.silver.ibge_desocupacao_silver
# MAGIC - workspace.silver.indicadores_economicos_silver
# MAGIC
# MAGIC ## Transformações aplicadas
# MAGIC - padronização de colunas
# MAGIC - criação de metadados de origem
# MAGIC - criação de colunas temporais
# MAGIC - unificação dos indicadores econômicos