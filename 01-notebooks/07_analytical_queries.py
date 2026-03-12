# Databricks notebook source
gold_macro_overview = "workspace.gold.gold_macro_overview"

print("Tabela analítica:", gold_macro_overview)

# COMMAND ----------

spark.sql(f"""
SELECT
    indicador,
    fonte,
    MIN(data_referencia) AS data_inicial,
    MAX(data_referencia) AS data_final,
    COUNT(*) AS total_registros,
    ROUND(MIN(valor), 2) AS valor_minimo,
    ROUND(MAX(valor), 2) AS valor_maximo,
    ROUND(AVG(valor), 2) AS valor_medio
FROM {gold_macro_overview}
GROUP BY indicador, fonte
ORDER BY indicador, fonte
""").show(truncate=False)

# COMMAND ----------

spark.sql(f"""
WITH ultima_data AS (
    SELECT
        indicador,
        MAX(data_referencia) AS ultima_data
    FROM {gold_macro_overview}
    GROUP BY indicador
)
SELECT
    g.indicador,
    g.fonte,
    g.data_referencia,
    ROUND(g.valor, 2) AS valor
FROM {gold_macro_overview} g
INNER JOIN ultima_data u
    ON g.indicador = u.indicador
   AND g.data_referencia = u.ultima_data
ORDER BY g.indicador
""").show(truncate=False)

# COMMAND ----------

spark.sql(f"""
SELECT
    ano,
    mes,
    data_referencia,
    ROUND(valor, 2) AS taxa_desocupacao
FROM {gold_macro_overview}
WHERE indicador = 'taxa_desocupacao'
ORDER BY data_referencia
""").show(20, truncate=False)

# COMMAND ----------

spark.sql(f"""
SELECT
    ano,
    mes,
    data_referencia,
    ROUND(valor, 2) AS ipca
FROM {gold_macro_overview}
WHERE indicador = 'ipca'
ORDER BY data_referencia
""").show(20, truncate=False)

# COMMAND ----------

spark.sql(f"""
SELECT
    ano,
    mes,
    data_referencia,
    ROUND(valor, 2) AS selic
FROM {gold_macro_overview}
WHERE indicador = 'selic'
ORDER BY data_referencia
""").show(20, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC # Analytical Queries
# MAGIC
# MAGIC Este notebook reúne consultas analíticas sobre a camada Gold do projeto.
# MAGIC
# MAGIC ## Fonte consultada
# MAGIC - workspace.gold.gold_macro_overview
# MAGIC
# MAGIC ## Objetivo
# MAGIC Demonstrar o consumo analítico dos indicadores econômicos processados no pipeline.