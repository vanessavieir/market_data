# market_data

Este projeto é parte integrante do curso **Sistemas para Internet**.
As contribuições devem ser discutidas com os criadores e podem incluir melhorias nos pipelines, análises adicionais, entre outros.

## Estrutura do Projeto

#### ├── market_data/   -  Pasta principal do projeto
##### │ ├── bronze/  -  Dados brutos e notebooks do pipeline de ETL para a camada Bronze
##### │ │ ├── supermarket_sales_v1.csv  -  Dados brutos na camada Bronze
##### │ ├── gold/  -  Tabelas finais e notebooks do pipeline de ETL para a camada Gold
##### │ │ ├── branch_and_global_rating -  Tabela final 1 na camada Gold
##### │ │ ├── customer_gender_age_count  -  Tabela final 2 na camada Gold
##### │ │ ├── payment_methods_data  -  Tabela final 2 na camada Gold
##### │ │ ├── product_cat_sales  -  Tabela final 2 na camada Gold
##### │ │ ├── product_profit_analysis  -  Tabela final 2 na camada Gold
##### │ │ ├── product_sales_summary  -  Tabela final 2 na camada Gold
##### │ │ ├── sales_by_branch_city  -  Tabela final 2 na camada Gold
##### │ │ ├── sales_schedule_table  -  Tabela final 2 na camada Gold
##### │ │ ├── customer_gender_age_count  -  Tabela final 2 na camada Gold
##### ├── dictionary.md  -  Dicionário de dados
##### ├── architecture.md  -  Documentação da arquitetura do projeto
##### ├── README.md  -  Este arquivo README


## Configuração do Ambiente Databricks

### Passos para Configuração:

1. **Importar Notebooks:** Importe os notebooks principais no Databricks, presentes nas pastas `market_data/bronze/` e `market_data/gold`.
2. **Configurar Clusters:** Os clusters já foram configurados pelos profissionais da NTT Data.


### Uso no Databricks

**Execução do Pipeline ETL** - **Camada Bronze**:
- Abra o notebook market_data/bronze/notebooks/etl_bronze no Databricks para o pipeline de ETL da camada Bronze.
- Execute as células do notebook para iniciar o pipeline ETL.

**Execução do Pipeline ETL - Camada Gold**:
- Abra o notebook market_data/gold/notebooks/etl_gold no Databricks para o pipeline de ETL da camada Gold.
- Execute as células do notebook para iniciar o pipeline ETL.

### Licença
Este projeto é desenvolvido para fins educacionais.

## Contato
Para mais informações, entre em contato com os integrantes do projeto:
- Artur Luna: [artur.00000845958@unicap.br]
- Charly Silva: [charly.2020205640@unicap.br]
- Diego Ednaldo: [diego.00000845949@unicap.br]
- Hernande Oliveira: [hernande.00000845962@unicap.br]
- Vanessa Ferreira: [vanessa.00000845942@unicap.br]


## Agradecimentos
Agradecemos aos profissionais da NTT Data pela orientação e suporte contínuo neste projeto acadêmico.

