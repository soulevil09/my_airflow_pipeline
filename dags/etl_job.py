from pyspark.sql import SparkSession
from pyspark.sql.functions import concat_ws, col, when, lit
import requests
import os

def run_pyspark_etl():
    spark = SparkSession.builder \
        .appName('Ler CSV do S3') \
        .config('spark.hadoop.fs.s3a.access.key', os.getenv('AWS_ACCESS_KEY_ID')) \
        .config('spark.hadoop.fs.s3a.secret.key', os.getenv('AWS_SECRET_ACCESS_KEY')) \
        .config('spark.hadoop.fs.s3a.endpoint', 's3.amazonaws.com') \
        .config('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk:1.11.1030') \
        .getOrCreate()

    bucket_name = 'meu-bucket-pyspark'
    file_key = 'pessoas_teste.csv'
    s3_path = f's3a://{bucket_name}/{file_key}'

    df = spark.read.csv(s3_path, header=True, inferSchema=True)

    df_altered = df.withColumn('Nome Completo', concat_ws(' ', col('Nome'), col('Sobrenome')))\
          .withColumn('Aposentadoria', when(df.Idade > 65, 'Elegível').otherwise('Inelegível'))

    # Salvar como CSV localmente
    local_csv = '/tmp/pessoas_teste_alterado.csv'
    df_altered.toPandas().to_csv(local_csv, index=False)

    return local_csv  # Retorna o caminho do arquivo gerado

def dados_cidade(cep):
    spark = SparkSession.builder\
        .appName('Dados da Cidade')\
        .getOrCreate()
    
    url_cep = f'https://viacep.com.br/ws/{cep}/json/'
    response_url_cep = requests.get(url_cep)


    if response_url_cep.status_code == 200:
        data_cep = [response_url_cep.json()]
        df = spark.read.json(spark.sparkContext.parallelize(data_cep))    
    else:
        raise Exception(f'Erro ao buscar CEP: {response_url_cep.status_code}')
    
    url_clima = f'https://api.hgbrasil.com/weather?key=f2a47678&city_name={data_cep[0]['localidade']}'
    response_url_clima = requests.get(url_clima)

    if response_url_clima.status_code == 200:
        data_clima =  response_url_clima.json()
        df = df.withColumn('Temperatura Atual', lit(data_clima['results']['temp']))\
               .withColumn('Condição do dia', lit(data_clima['results']['description']))
    else:
        raise Exception(f'Erro ao buscar Clima: {response_url_clima}')

    
    local_csv = '/tmp/dados_cidade.csv'
    df.toPandas().to_csv(local_csv, index=False)
        

    return local_csv

