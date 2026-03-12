# Databricks notebook source
catalog_name = "workspace"

silver_unificada_table = f"{catalog_name}.silver.indicadores_economicos_silver"

gold_dim_tempo = f"{catalog_name}.gold.dim_tempo"
gold_dim_indicador = f"{catalog_name}.gold.dim_indicador"
gold_fato_indicadores = f"{catalog_name}.gold.fato_indicadores"
gold_macro_overview = f"{catalog_name}.gold.gold_macro_overview"

print("Silver unificada:", silver_unificada_table)
print("Gold dim_tempo:", gold_dim_tempo)
print("Gold dim_indicador:", gold_dim_indicador)
print("Gold fato_indicadores:", gold_fato_indicadores)
print("Gold macro_overview:", gold_macro_overview)

# COMMAND ----------

from pyspark.sql import functions as F

df_silver = spark.table(silver_unificada_table)

print("Silver unificada lida com sucesso.")

# COMMAND ----------

df_silver.printSchema()
df_silver.show(10, truncate=False)

# COMMAND ----------

df_dim_tempo = (
    df_silver
    .select("data_referencia", "ano", "mes", "trimestre", "ano_mes")
    .dropDuplicates()
    .withColumn("id_tempo", F.monotonically_increasing_id())
    .withColumn("nome_mes", F.date_format("data_referencia", "MMMM"))
    .select(
        "id_tempo",
        "data_referencia",
        "ano",
        "mes",
        "trimestre",
        "ano_mes",
        "nome_mes"
    )
)

print("Dimensão tempo criada em memória.")

# COMMAND ----------

df_dim_tempo.printSchema()
df_dim_tempo.show(10, truncate=False)

# COMMAND ----------

df_dim_tempo.write.format("delta").mode("overwrite").saveAsTable(gold_dim_tempo)

print(f"Tabela {gold_dim_tempo} gravada com sucesso.")

# COMMAND ----------

df_dim_indicador = (
    df_silver
    .select("indicador", "fonte", "codigo_origem", "descricao_origem")
    .dropDuplicates()
    .withColumn("id_indicador", F.monotonically_increasing_id())
    .select(
        "id_indicador",
        "indicador",
        "fonte",
        "codigo_origem",
        "descricao_origem"
    )
)

print("Dimensão indicador criada em memória.")

# COMMAND ----------

df_dim_indicador.printSchema()
df_dim_indicador.show(20, truncate=False)

# COMMAND ----------

df_dim_indicador.write.format("delta").mode("overwrite").saveAsTable(gold_dim_indicador)

print(f"Tabela {gold_dim_indicador} gravada com sucesso.")

# COMMAND ----------

df_fato_indicadores = (
    df_silver.alias("s")
    .join(
        df_dim_tempo.alias("t"),
        on="data_referencia",
        how="left"
    )
    .join(
        df_dim_indicador.alias("i"),
        on=["indicador", "fonte", "codigo_origem", "descricao_origem"],
        how="left"
    )
    .select(
        F.col("t.id_tempo"),
        F.col("i.id_indicador"),
        F.col("s.data_referencia"),
        F.col("s.valor"),
        F.col("s.data_ingestao")
    )
)

print("Fato de indicadores criada em memória.")

# COMMAND ----------

df_dim_indicador = (
    df_silver
    .withColumn(
        "descricao_indicador",
        F.when(F.col("fonte") == "IBGE_SIDRA", F.lit("taxa_desocupacao"))
         .otherwise(F.col("descricao_origem"))
    )
    .select("indicador", "fonte", "codigo_origem", "descricao_indicador")
    .dropDuplicates()
    .withColumn("id_indicador", F.monotonically_increasing_id())
    .select(
        "id_indicador",
        "indicador",
        "fonte",
        "codigo_origem",
        "descricao_indicador"
    )
)

print("Dimensão indicador recriada corretamente em memória.")

# COMMAND ----------

df_dim_indicador.printSchema()
df_dim_indicador.show(20, truncate=False)

# COMMAND ----------

df_dim_indicador.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(gold_dim_indicador)

print(f"Tabela {gold_dim_indicador} regravada com sucesso.")

# COMMAND ----------

spark.table(gold_dim_indicador).printSchema()
spark.table(gold_dim_indicador).show(20, truncate=False)

# COMMAND ----------

df_fato_indicadores = (
    df_silver.alias("s")
    .withColumn(
        "descricao_indicador",
        F.when(F.col("fonte") == "IBGE_SIDRA", F.lit("taxa_desocupacao"))
         .otherwise(F.col("descricao_origem"))
    )
    .join(
        df_dim_tempo.alias("t"),
        on="data_referencia",
        how="left"
    )
    .join(
        df_dim_indicador.alias("i"),
        on=[
            "indicador",
            "fonte",
            "codigo_origem",
            "descricao_indicador"
        ],
        how="left"
    )
    .select(
        F.col("t.id_tempo"),
        F.col("i.id_indicador"),
        F.col("s.data_referencia"),
        F.col("s.valor"),
        F.col("s.data_ingestao")
    )
)

print("Fato de indicadores recriada em memória.")

# COMMAND ----------

df_fato_indicadores.printSchema()
df_fato_indicadores.show(10, truncate=False)

# COMMAND ----------

df_fato_indicadores.write.format("delta").mode("overwrite").saveAsTable(gold_fato_indicadores)

print(f"Tabela {gold_fato_indicadores} gravada com sucesso.")

# COMMAND ----------

df_gold_macro = (
    df_fato_indicadores.alias("f")
    .join(df_dim_tempo.alias("t"), on="id_tempo", how="left")
    .join(df_dim_indicador.alias("i"), on="id_indicador", how="left")
    .select(
        "f.id_tempo",
        "f.id_indicador",
        "i.indicador",
        "i.fonte",
        "i.codigo_origem",
        "i.descricao_indicador",
        "t.data_referencia",
        "t.ano",
        "t.mes",
        "t.trimestre",
        "t.ano_mes",
        "t.nome_mes",
        "f.valor",
        "f.data_ingestao"
    )
)

print("Gold macro overview criada em memória.")

# COMMAND ----------

df_gold_macro.printSchema()
df_gold_macro.show(10, truncate=False)

# COMMAND ----------

df_gold_macro.write.format("delta").mode("overwrite").saveAsTable(gold_macro_overview)

print(f"Tabela {gold_macro_overview} gravada com sucesso.")

# COMMAND ----------

spark.sql(f"""
SELECT
    indicador,
    fonte,
    MIN(data_referencia) AS data_min,
    MAX(data_referencia) AS data_max,
    COUNT(*) AS total_registros
FROM {gold_macro_overview}
GROUP BY indicador, fonte
ORDER BY indicador, fonte
""").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC # Silver to Gold
# MAGIC
# MAGIC Este notebook realiza a transformação da camada Silver para a camada Gold do projeto.
# MAGIC
# MAGIC ## Entrada
# MAGIC - workspace.silver.indicadores_economicos_silver
# MAGIC
# MAGIC ## Saídas
# MAGIC - workspace.gold.dim_tempo
# MAGIC - workspace.gold.dim_indicador
# MAGIC - workspace.gold.fato_indicadores
# MAGIC - workspace.gold.gold_macro_overview
# MAGIC
# MAGIC ## Transformações aplicadas
# MAGIC - criação de dimensão tempo
# MAGIC - criação de dimensão indicador
# MAGIC - criação da fato de indicadores
# MAGIC - criação de visão analítica final