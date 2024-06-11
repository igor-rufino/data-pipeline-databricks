# Projeto: Data Pipeline com Spark e Delta Lake

O projeto `data-pipeline-databricks` implementa um pipeline de dados utilizando Apache Spark e Delta Lake no ambiente Databricks. O pipeline é dividido em três etapas principais: processamento de dados brutos (Bronze), dados processados e limpos (Silver) e dados analíticos agregados (Gold). Cada etapa é gerenciada por um notebook Databricks específico.

## Estrutura do Projeto

**Bronze:** Ingestão e armazenamento inicial dos dados brutos.
**Silver:** Transformação e limpeza dos dados para análise.
**Gold:** Agregação e preparação dos dados para relatórios e visualizações.

## Notebooks

Notebooks exportandos do ambiente do Databricks:

- bronze_notebook
- silver_notebook
- gold_notebook

### bronze_notebook.py

Este notebook é responsável pela ingestão dos dados brutos e armazenamento em tabelas Delta na camada Bronze.

**Funcionalidades:**

1. **Inicia uma sessão Spark:** Configura a sessão do Spark para o aplicativo de pipeline de dados.
2. **Lista os arquivos no diretório de aterrissagem:** Identifica e filtra os arquivos para processamento.
3. **Processa e carrega os dados:** Lê os arquivos CSV, adiciona a coluna processing_date e armazena os dados como tabelas Delta.
4. **Relata as tabelas criadas:** Exibe a lista de tabelas criadas no catálogo Databricks.

### silver_notebook.py

Este notebook transforma os dados da camada Bronze para a camada Silver, aplicando deduplicação e limpeza.

**Funcionalidades:**

1. **Inicia uma sessão Spark:** Configura a sessão do Spark para o aplicativo de pipeline de dados.
2. **Lista e prepara as tabelas Bronze:** Identifica e ordena as tabelas relevantes para processamento.
3. **Realiza transformações e limpeza:** Deduplica registros e formata colunas.
4. **Atualiza ou cria tabelas Silver:** Aplica operações de merge (upsert) para dados incrementais ou sobrescreve tabelas para cargas completas.
5. **Relata as tabelas Silver criadas:** Exibe a lista de tabelas Silver no catálogo Databricks.

### gold_notebook.py

Este notebook consolida e agrega os dados da camada Silver, preparando-os para análise e relatórios na camada Gold.

**Funcionalidades:**

1. **Inicia uma sessão Spark:** Configura a sessão do Spark para o aplicativo de pipeline de dados.
2. **Lê as tabelas Silver:** Carrega os dados limpos e processados da camada Silver.
3. **Consolida dados:** Combina dados de diferentes cargas e remove duplicatas.
4. **Cria visão analítica:** Agrega dados de transações por cliente.
5. **Atualiza ou cria tabelas Gold:** Aplica operações de merge (upsert) ou sobrescreve as tabelas Gold com os dados mais recentes.

## Requisitos

- **Databricks:** Plataforma de dados unificada que facilita a execução dos notebooks.
- **Apache Spark:** Motor de processamento de dados distribuído.
- **Dados:** Tabelas csv dos dados de Consumidores e Transações.

## Como Executar

1. **Upload dos Notebooks:** Carregue os notebooks no ambiente Databricks.
2. **Configuração dos Caminhos de Dados:** Verifique e ajuste os caminhos de dados conforme necessário.
3. **Execução dos Notebooks:** Execute os notebooks na ordem bronze_notebook, silver_notebook e gold_notebook.
4. **Verificação de Resultados:** Use os comandos finais de cada notebook para verificar as tabelas criadas no catálogo Databricks.