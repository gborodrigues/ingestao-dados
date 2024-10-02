from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, regexp_replace, upper, lit, when
import mysql.connector
import logging
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType
import os
from datetime import datetime

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
    df = df.withColumn("campo_limpo", when(col("campo_limpo") == "", "VAZIO").otherwise(col("campo_limpo")))
    return df

db_config = {
    'user': 'root',
    'password': 'root',
    'database': 'ingestao_dados',
    'host': 'exe8-db-1'
}

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

spark = SparkSession.builder \
    .appName("KafkaStructuredStreamingExample") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .getOrCreate()

kafka_bootstrap_servers = "kafka:29092"
kafka_topic = "gestao"

schema = StructType([
    StructField("Ano", IntegerType(), True),
    StructField("Trimestre", StringType(), True),
    StructField("Categoria", StringType(), True),
    StructField("Tipo", StringType(), True),
    StructField("CNPJ IF", StringType(), True),
    StructField("Instituição financeira", StringType(), True),
    StructField("Índice", StringType(), True),
    StructField("Quantidade de reclamacoes reguladas procedentes", FloatType(), True),
    StructField("Quantidade de reclamacoes reguladas - outras", FloatType(), True),
    StructField("Quantidade de reclamacoes não reguladas", FloatType(), True),
    StructField("Quantidade total de reclamacoes", FloatType(), True),
    StructField("Quantidade total de clientes – CCS e SCR", StringType(), True),
    StructField("Quantidade de clientes – CCS", StringType(), True),
    StructField("Quantidade de clientes – SCR", StringType(), True),
    StructField("campo_limpo", StringType(), True),
])


#.option("maxOffsetsPerTrigger", 1)
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic) \
    .option("startingOffsets", "earliest") \
    .load()

df_parsed = df \
    .selectExpr("CAST(value AS STRING)") \
    .withColumn("json_data", from_json(col("value"), schema)) \
    .select("json_data.*")

def process_batch(df, epoch_id):
    df_cleaned = clean_string(df, "Instituição financeira")

    conn = mysql.connector.connect(**db_config)
    cursor = conn.cursor()

    try:
        processed_rows = []

        for row in df_cleaned.collect():
            instituicao = row['campo_limpo']
            
            cursor.execute(f"SELECT campo_limpo FROM Bancos WHERE campo_limpo = %s", (instituicao,))
            result = cursor.fetchone()
            
            if result:
                campo_limpo = result[0]
                logger.info(f"Matched institution: {instituicao}, Campo_Limpo: {campo_limpo}")
                
                row_dict = row.asDict()
                row_dict['campo_limpo'] = campo_limpo
                processed_rows.append(row_dict)
            else:
                logger.warning(f"No match found for institution: {instituicao}")

        if processed_rows:

            logger.info(f"Processed rows: {processed_rows}")

            processed_df = spark.createDataFrame(processed_rows, schema)

            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"dados_processados_{timestamp}_{epoch_id}.parquet"
            
            output_path = os.path.join("/opt/bitnami/spark/dados_processados3", filename)
            processed_df.write \
                .mode("append") \
                .parquet(output_path)

            logger.info(f"Wrote {len(processed_rows)} rows to Parquet file: {filename}")
        else:
            logger.info("No data to write to Parquet file in this batch")

    except Exception as e:
        logger.error(f"Error processing batch: {str(e)}")
    finally:
        cursor.close()
        conn.close()

query = df_parsed.writeStream \
    .foreachBatch(process_batch) \
    .start()

query.awaitTermination()