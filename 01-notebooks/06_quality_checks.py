# Databricks notebook source
gold_macro_overview = "workspace.gold.gold_macro_overview"
silver_unificada_table = "workspace.silver.indicadores_economicos_silver"

print("Gold:", gold_macro_overview)
print("Silver:", silver_unificada_table)

# COMMAND ----------

df_gold = spark.table(gold_macro_overview)
df_silver = spark.table(silver_unificada_table)

print("Tabelas carregadas com sucesso.")

# COMMAND ----------

from pyspark.sql import functions as F

df_nulos = df_gold.select(
    F.count(F.when(F.col("indicador").isNull(), 1)).alias("nulos_indicador"),
    F.count(F.when(F.col("fonte").isNull(), 1)).alias("nulos_fonte"),
    F.count(F.when(F.col("data_referencia").isNull(), 1)).alias("nulos_data_referencia"),
    F.count(F.when(F.col("valor").isNull(), 1)).alias("nulos_valor")
)

df_nulos.show()

# COMMAND ----------

df_duplicados = spark.sql(f"""
SELECT
    indicador,
    fonte,
    data_referencia,
    COUNT(*) AS qtd
FROM {gold_macro_overview}
GROUP BY indicador, fonte, data_referencia
HAVING COUNT(*) > 1
ORDER BY qtd DESC
""")

df_duplicados.show(truncate=False)

# COMMAND ----------

spark.sql(f"""
SELECT
    indicador,
    fonte,
    COUNT(*) AS total_registros
FROM {gold_macro_overview}
GROUP BY indicador, fonte
ORDER BY indicador, fonte
""").show(truncate=False)

# COMMAND ----------

spark.sql(f"""
SELECT
    indicador,
    fonte,
    MIN(data_referencia) AS data_min,
    MAX(data_referencia) AS data_max
FROM {gold_macro_overview}
GROUP BY indicador, fonte
ORDER BY indicador, fonte
""").show(truncate=False)

# COMMAND ----------

spark.sql(f"""
SELECT
    indicador,
    fonte,
    ROUND(MIN(valor), 2) AS valor_min,
    ROUND(MAX(valor), 2) AS valor_max,
    ROUND(AVG(valor), 2) AS valor_medio
FROM {gold_macro_overview}
GROUP BY indicador, fonte
ORDER BY indicador, fonte
""").show(truncate=False)

# COMMAND ----------

qtd_silver = df_silver.count()
qtd_gold = df_gold.count()

print("Quantidade Silver:", qtd_silver)
print("Quantidade Gold:", qtd_gold)

# COMMAND ----------

resultado_checks = [
    ("nulos_colunas_criticas", "OK se todos = 0"),
    ("duplicidade_indicador_fonte_data", "OK se sem linhas retornadas"),
    ("volume_por_indicador", "Validado"),
    ("intervalo_temporal", "Validado"),
    ("faixa_de_valores", "Validado"),
    ("silver_vs_gold", "OK se contagens iguais")
]

for check, status_esperado in resultado_checks:
    print(f"{check}: {status_esperado}")

# COMMAND ----------

# MAGIC %md
# MAGIC # Quality Checks
# MAGIC
# MAGIC Este notebook reúne verificações básicas de qualidade sobre as camadas Silver e Gold do projeto.
# MAGIC
# MAGIC ## Tabelas avaliadas
# MAGIC - workspace.silver.indicadores_economicos_silver
# MAGIC - workspace.gold.gold_macro_overview
# MAGIC
# MAGIC ## Verificações aplicadas
# MAGIC - valores nulos em colunas críticas
# MAGIC - duplicidade por indicador, fonte e data
# MAGIC - volume de registros por indicador
# MAGIC - intervalo temporal por indicador
# MAGIC - faixa de valores
# MAGIC - consistência de volume entre Silver e Gold