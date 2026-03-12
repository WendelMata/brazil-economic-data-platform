# Brazil Economic Data Platform

Pipeline de Engenharia de Dados com arquitetura Medallion construído no Databricks para ingestão, transformação, modelagem e análise de indicadores econômicos brasileiros a partir de APIs públicas do Banco Central do Brasil e do IBGE.

## Visão geral

Este projeto foi desenvolvido com o objetivo de demonstrar, na prática, a construção de um pipeline de dados completo em camadas Bronze, Silver e Gold, utilizando Databricks, PySpark e Delta Lake.

A solução realiza a ingestão de dados econômicos de múltiplas fontes públicas, padroniza as estruturas, consolida os indicadores em uma camada analítica e aplica verificações básicas de qualidade para garantir consistência e confiabilidade dos dados.

## Objetivo de negócio

Centralizar indicadores econômicos brasileiros em uma estrutura analítica organizada, permitindo consultas e análises sobre o comportamento de variáveis como:

- Selic
- IPCA
- Taxa de desocupação

A proposta é criar uma base que facilite o consumo analítico e sirva como fundação para dashboards, estudos econômicos e futuras ampliações do pipeline.

## Arquitetura

O projeto foi estruturado seguindo o padrão Medallion:

### Bronze
Camada de ingestão de dados brutos provenientes das APIs.

### Silver
Camada de padronização, enriquecimento e unificação dos indicadores.

### Gold
Camada analítica com modelagem dimensional e visão final para consumo.

## Stack utilizada

- Databricks Free Edition
- PySpark
- Spark SQL
- Delta Lake
- Python
- Pandas
- APIs públicas do Banco Central do Brasil
- API SIDRA / IBGE

## Fontes de dados

### Banco Central do Brasil
API SGS para séries temporais econômicas.

Indicadores utilizados:
- Selic (código 432)
- IPCA (código 433)

### IBGE
API SIDRA para indicadores estatísticos.

Indicador utilizado:
- Taxa de desocupação
- Tabela 6381
- Variável 4099

## Pipeline de dados

### Bronze

Tabelas criadas:
- `workspace.bronze.bcb_series_bronze`
- `workspace.bronze.ibge_pnad_desocupacao_bronze`

Responsabilidades:
- ingestão das APIs
- tipagem inicial
- persistência em Delta Lake

### Silver

Tabelas criadas:
- `workspace.silver.bcb_series_silver`
- `workspace.silver.ibge_desocupacao_silver`
- `workspace.silver.indicadores_economicos_silver`

Responsabilidades:
- padronização de colunas
- criação de metadados de origem
- criação de colunas temporais
- unificação dos indicadores

### Gold

Tabelas criadas:
- `workspace.gold.dim_tempo`
- `workspace.gold.dim_indicador`
- `workspace.gold.fato_indicadores`
- `workspace.gold.gold_macro_overview`

Responsabilidades:
- modelagem dimensional
- criação da fato de indicadores
- consolidação da visão analítica final

## Qualidade dos dados

Foram implementadas verificações básicas de qualidade sobre as camadas Silver e Gold:

- checagem de nulos em colunas críticas
- checagem de duplicidade por indicador, fonte e data
- validação de volume de registros por indicador
- validação de intervalo temporal por indicador
- análise de faixa de valores
- conferência de consistência entre Silver e Gold

## Resultados do pipeline

Indicadores consolidados na camada Gold:
- IPCA
- Selic
- Taxa de desocupação

Validação final:
- IPCA: 120 registros
- Selic: 3653 registros
- Taxa de desocupação: 154 registros

## Consultas analíticas

O projeto inclui consultas analíticas sobre a camada Gold para demonstrar o consumo dos dados, incluindo:

- visão geral por indicador
- último valor disponível por indicador
- séries temporais para taxa de desocupação
- séries temporais para IPCA
- séries temporais para Selic

## Estrutura do projeto

```text
brazil-economic-data-platform/
├── 01-notebooks/
│   ├── 01_setup_environment
│   ├── 02_ingest_bcb
│   ├── 03_ingest_ibge
│   ├── 04_bronze_to_silver
│   ├── 05_silver_to_gold
│   ├── 06_quality_checks
│   └── 07_analytical_queries
├── 02-docs/
├── 03-sql/
└── 04-exports/
