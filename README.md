# Brazil Economic Data Platform

![Databricks](https://img.shields.io/badge/Databricks-Data%20Platform-orange)
![PySpark](https://img.shields.io/badge/PySpark-Big%20Data-yellow)
![Delta Lake](https://img.shields.io/badge/Delta-Lake-blue)
![SQL](https://img.shields.io/badge/SQL-Analytics-lightgrey)
![Data Engineering](https://img.shields.io/badge/Data-Engineering-green)

Pipeline de engenharia de dados desenvolvido em **Databricks + PySpark** utilizando arquitetura **Medallion (Bronze → Silver → Gold)** para ingestão, transformação e análise de indicadores macroeconômicos do Brasil.

O projeto integra dados de duas fontes oficiais:

* Banco Central do Brasil (BCB / SGS)
* IBGE (SIDRA)

Indicadores utilizados:

* IPCA
* Taxa SELIC
* Taxa de Desocupação

---

# Arquitetura do Projeto

![Architecture Diagram](02-docs/images/architecture_medallion_pipeline.png)

O pipeline segue o padrão **Medallion Architecture (Bronze → Silver → Gold)**, onde os dados são ingeridos em estado bruto, transformados e padronizados na camada Silver, e modelados analiticamente na camada Gold para consumo analítico.
---

# Estrutura do Projeto

```
01-notebooks
├── 02_ingest_bcb
├── 03_ingest_ibge
├── 04_bronze_to_silver
├── 05_silver_to_gold
├── 06_quality_checks
└── 07_analytical_queries

02-docs
└── images
```

---

# Pipeline de Dados

## Bronze Layer

### Fonte BCB (IPCA e SELIC)

![Bronze BCB](02-docs/images/bronze_bcb.png)

### Fonte IBGE (Taxa de Desocupação)

![Bronze IBGE](02-docs/images/bronze_ibge.png)

---

## Silver Layer

Tabela unificada com os indicadores econômicos.

![Silver Overview](02-docs/images/silver_overview.png)

---

## Gold Layer

Modelo analítico consolidado com:

* dimensão tempo
* dimensão indicador
* fato indicadores
* visão analítica macroeconômica

![Gold Overview](02-docs/images/gold_overview.png)

---

# Quality Checks

Validações implementadas:

* verificação de valores nulos
* verificação de duplicidade
* consistência de volume entre Silver e Gold
* validação de intervalo temporal
* análise de faixa de valores

![Quality Checks](02-docs/images/quality_checks.png)

---

# Análises Geradas

Exemplo de consulta analítica sobre os indicadores.

![Analytical Queries](02-docs/images/analytical_queries.png)

---

# Tecnologias Utilizadas

* Databricks
* PySpark
* Delta Lake
* SQL
* APIs públicas (BCB / IBGE)

---

# Autor

Wendel Mata  
Data Engineering Student

LinkedIn:
https://www.linkedin.com/in/wendel-mata-b31633206/
