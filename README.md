Airflow Pipeline com PySpark

Este repositório contém dois pipelines de dados criados com Apache Airflow e Pyspark, rodando em um ambiente Linux Ubuntu

## Pipelines
-**pyspark_etl_pipeline**: Recupera arquivo .csv do bucket, realiza a transformação e carrega novamente para uma nova pasta do bucket o arquivo alterado
-**dados_cidade_pipeline**: Usa a Api ViaCEP para recuperar dados de um CEP fornecido como parâmetro e com o nome da cidade recupera dados climáticos da API hgbrasil, unifica os valores dentro de um Dataframe e envia por email um arquivo .csv com as informações  

## Estrutura do Projeto
-**dags/**: Contém os arquivos python com as DAGs configuradas
-**requirements/**: Lista de Dependências para rodar o projeto

## Pré-requisitos
-Python 3.12.3
-Java openjdk 11.0.25
-Spark version 3.5.3

## Como configurar
1. Clone esse repositorio:
```bash
    git clone https://github.com/soulevil09/my_airflow_pipeline.git
    cd my_airflow_pipeline.git
    python3 -m venv venv
    source venv/bin/activate
    pip install -r requirements.txt
    export AIRFLOW_HOME=$(pwd)/setup
    airflow db init
    airflow users create -u admin -p admin -r Admin -e admin@admin.com
    airflow webserver -p 8080
    airflow scheduler