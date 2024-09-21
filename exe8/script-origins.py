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
    StructField("Quantidade total de clientes - CCS e SCR", StringType(), True),
    StructField("Quantidade de clientes - CCS", StringType(), True),
    StructField("Quantidade de clientes - SCR", StringType(), True),
    StructField("Unnamed: 14", StringType(), True),
])

df_parsed = df \
    .selectExpr("CAST(value AS STRING)") \
    .withColumn("json_data", from_json(col("value"), schema)) \
    .select("json_data.*")

df_parsed = clean_string(df_parsed, "Instituicao financeira")

def process_batch(batch_df, batch_id):
    cursor = conn.cursor()
    
    cleaned_names = batch_df.select("Instituicao financeira").distinct().collect()
    
    for row in cleaned_names:
        # Check if the processed name already exists in the database
        cursor.execute(f"SELECT COUNT(*) FROM {table_name} WHERE campo_limpo = '{row[0]}';")
        exists = cursor.fetchone()[0] > 0
        
        if not exists:
            # If not, proceed with processing
            cursor.execute(f"SELECT * FROM {table_name} WHERE campo_limpo = '{row[0]}';")
            dataBancos = cursor.fetchall()

            columns = [desc[0] for desc in cursor.description]
            dfBancos = spark.createDataFrame(dataBancos, schemaBancos)
            
            merged_df = dfBancos.join(batch_df, on=["campo_limpo"], how="inner")
            
            merged_df.write.mode('append').parquet('dados_finais.parquet')
    
    cursor.close()


query = df_parsed.writeStream \
    .outputMode("append") \
    .foreachBatch(process_batch) \
    .start()

query.awaitTermination()
