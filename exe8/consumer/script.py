from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, regexp_replace, upper
import mysql.connector
import logging
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType

def clean_string(df, field):
    pattern = (
        r' - PRUDENCIAL|'
        r' S\.A[./]?|'
        r' S/A[/]?|'
        r'GRUPO|'
        r' SCFI|'
        r' CC |'
        r' C\.C |'
        r' CCTVM[/]?|'
        r' LTDA[/]?|'
        r' DTVM[/]?|'
        r' BM[/]?|'
        r' CH[/]?|'
        r'COOPERATIVA DE CRÉDITO, POUPANÇA E INVESTIMENTO D[E?O?A/]?|'
        r' [(]conglomerado[)]?|'
        r'GRUPO[ /]|'
        r' -[ /]?'
    )
    df = df.withColumn("campo_limpo", regexp_replace(col(field), pattern, ''))

    df = df.withColumn("campo_limpo", upper(col("campo_limpo")))
    
    return df

db_config = {
    'user': 'root',
    'password': 'root',
    'database': 'ingestao_dados',
    'host': 'exe8-db-1'
}

logger = logging.getLogger('pyspark')
logger.info("My test info statement")

conn = mysql.connector.connect(**db_config)

table_name = 'Bancos'

spark = SparkSession.builder \
    .appName("KafkaStructuredStreamingExample") \
    .getOrCreate()

kafka_bootstrap_servers = "kafka:29092" 
kafka_topic = "gestao"              

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic) \
    .option("startingOffsets", "earliest") \
    .option("maxOffsetsPerTrigger", 1)  \
    .load()

df.show()
input("Pressione Enter para continuar...")

schemaBancos = StructType([
    StructField("Segmento", StringType(), True),
    StructField("CNPJ", StringType(), True),
    StructField("Nome", StringType(), True),
    StructField("campo_limpo", StringType(), True)
])

schema = StructType([
    StructField("Ano", IntegerType(), True),
    StructField("Trimestre", StringType(), True),
    StructField("Categoria", StringType(), True),
    StructField("Tipo", StringType(), True),
    StructField("CNPJ IF", StringType(), True),
    StructField("Instituicao financeira", StringType(), True),
    StructField("Índice", StringType(), True),
    StructField("Quantidade de reclamacoes reguladas procedentes", FloatType(), True),
    StructField("Quantidade de reclamacoes reguladas - outras", FloatType(), True),
    StructField("Quantidade de reclamacoes não reguladas", FloatType(), True),
    StructField("Quantidade total de reclamacoes", FloatType(), True),
    StructField("Quantidade total de clientes – CCS e SCR", StringType(), True),
    StructField("Quantidade de clientes – CCS", StringType(), True),
    StructField("Quantidade de clientes – SCR", StringType(), True),
    StructField("Unnamed: 14", StringType(), True)
])

df_parsed = df \
    .selectExpr("CAST(value AS STRING)") \
    .withColumn("json_data", from_json(col("value"), schema)) \
    .select("json_data.*")

print ("CAMINHAO DO LEITE")

df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()
print ("CAMINHAO DO LEITE")

df_parsed = clean_string(df_parsed, "Instituicao financeira")

print ("--------------------------------------------")
sal = df_parsed.select(col("Instituicao financeira"))
sal.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()
input("Pressione Enter para continuar...")
print ("TESTE")

cursor = conn.cursor()

query = "SELECT * FROM Bancos WHERE campo_limpo = 'BANCO DO BRASIL' LIMIT 1 "

# Executa a consulta
cursor.execute(query)

# Recupera os resultados
results = cursor.fetchall()

# Exibe os resultados
for row in results:
    print(row)

# Aguarda pressionamento de Enter antes de continuar
#input("Pressione Enter para continuar...")

# cursor.execute(f"SELECT * FROM {table_name} WHERE campo_limpo = '{df_parsed.select(col("Instituicao financeira"))}';")
cursor.execute(f"SELECT Segmento FROM Bancos WHERE campo_limpo = 'BANCO DO BRASIL' LIMIT 1;")

print("dahudahudsauhdhuada")
print(cursor.fetchall())


#input("Pressione Enter para continuar...")
dataBancos = cursor.fetchall()
dfBancos = spark.createDataFrame(dataBancos, schemaBancos)

merged_df = dfBancos.join(df_parsed, on=["campo_limpo"], how="inner")


merged_df.writeStream \
    .format("parquet") \
    .option("path", '/opt/bitnami/spark') \
    .option("checkpointLocation", '/opt/bitnami/spark/dados_finais.parquet') \
    .outputMode("append") \
    .start()


query = merged_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

# merged_df = dfBancos.join(df_parsed, on=["campo_limpo"], how="inner")

# merged_df.write.parquet(f'dados_finais.parquet', mode='append')
print (sal)
print ("B;A")

print (cursor.fetchall())

print ("BlA")

query.awaitTermination()


