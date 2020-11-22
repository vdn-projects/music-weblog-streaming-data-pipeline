import logging
import logging.config
from configparser import ConfigParser

from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as psf
import subprocess

import os

"""
{
    "ts": 1538362104000,    
    "userId": "78", 
    "sessionId": 187, 
    "page": "Home", 
    "auth": "Logged In", 
    "method": "GET", 
    "status": 200, 
    "level": "free", 
    "itemInSession": 24, 
    "location": "McAllen-Edinburg-Mission, TX", 
    "userAgent": "Mozilla/5.0 (Windows NT 6.1; WOW64)", 
    "lastName": "Farley", 
    "firstName": "Ainsley", 
    "registration": 1538304455000, 
    "gender": "F",

}
"""
def run_cmd(args_list):
    """
    run linux commands
    """
    # import subprocess
    # print('Running system command: {0}'.format(' '.join(args_list)))
    proc = subprocess.Popen(args_list, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    s_output, s_err = proc.communicate()
    s_return =  proc.returncode
    
    if s_return != 0:
        return s_return, s_output, s_err
    else:
        return "Command executed successfully!"

def insert_batch(df, epoch_id, target_table):
    df = df.withColumn("part_date", psf.from_unixtime(psf.col("ts")/1000, "YYYYMMdd"))
    df.coalesce(1).write.partitionBy("part_date").format("parquet").mode("append").saveAsTable(target_table)
    run_cmd(["impala-shell", "-i", "bdp-worker01-pdc.vn.prod", "-k", "--ssl", "-q", f"REFRESH {target_table}"])

def run_spark_job(spark: SparkSession, config: ConfigParser):
    """
    Run Spark Structured Streaming job reading data from Kafka
    """

    # set log level for Spark app
    spark.sparkContext.setLogLevel("WARN")

    # define schema for incoming data
    kafka_schema = StructType([
        StructField("ts", LongType(), True),
        StructField("userId", StringType(), True),
        StructField("sessionId", IntegerType(), True),
        StructField("page", StringType(), True),
        StructField("itemInSession", IntegerType(), True),
        StructField("auth", StringType(), True),
        StructField("method", StringType(), True),
        StructField("status", StringType(), True),
        StructField("level", StringType(), True),
        StructField("location", StringType(), True),
        StructField("userAgent", StringType(), True),
        StructField("lastName", StringType(), True),
        StructField("firstName", StringType(), True),
        StructField("registration", LongType(), True),
        StructField("gender", StringType(), True),
        StructField("artist", StringType(), True),
        StructField("song", StringType(), True),
        StructField("length", DoubleType(), True)
    ])

    # start reading data from Kafka
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", config.get("spark", "bootstrap_servers")) \
        .option("subscribe", config.get("kafka", "topic")) \
        .option("startingOffsets", config.get("spark", "starting_offsets")) \
        .option("maxOffsetsPerTrigger", config.get("spark", "max_offsets_per_trigger")) \
        .option("maxRatePerPartition", config.get("spark", "max_rate_per_partition")) \
        .option("stopGracefullyOnShutdown", "true") \
        .load()

    # print schema of incoming data
    logging.debug("Printing schema of incoming data")
    df.printSchema()

    # extract value of incoming Kafka data, ignore key
    kafka_df = df.selectExpr("CAST(value AS STRING)")

    service_table = kafka_df \
        .select(psf.from_json(kafka_df.value, kafka_schema).alias("DF")) \
        .select("DF.*")

    # query = service_table.writeStream.trigger(processingTime="20 seconds").format("console").option("truncate", "false").start()
    service_table.writeStream.format("console").option("truncate", "false").start()

    target_table = config.get("spark", "target_table")
    if config.get("spark", "checkpoint_remove") == "True":
        run_cmd(["hdfs", "dfs", "-rm", "-r", config.get("spark", "checkpoint_dir")])
    if config.get("spark", "drop_table") == "True":
        spark.sql(f"drop table if exists {target_table}")

    query = (service_table
                .writeStream
                .outputMode("append")
                .option("checkpointLocation", config.get("spark", "checkpoint_dir"))
                .foreachBatch(lambda df, epochId: insert_batch(df, epochId, target_table)
            ).start())

    query.awaitTermination()

if __name__ == "__main__":
    cur_path = os.getcwd()

    # load config
    config = ConfigParser()
    config.read(os.path.join(cur_path, "app.cfg"))

    # start logging
    logging.config.fileConfig(os.path.join(cur_path, "logging.ini"))
    logger = logging.getLogger(__name__)

    # create spark session
    spark = SparkSession \
        .builder \
        .master(config.get("spark", "master")) \
        .appName("itbi.streaming.consumer") \
        .getOrCreate()

    logger.info("Starting Spark Job")
    run_spark_job(spark, config)

    logger.info("Closing Spark Session")
    spark.stop()
