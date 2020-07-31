import time
import json
import logging
from pyspark.sql.types import *
from pyspark.sql import SparkSession
import pyspark.sql.functions as psf


# TODO Create a schema for incoming resources
schema = StructType([StructField("crime_id", StringType(), True),
                     StructField("original_crime_type_name", StringType(), True),
                     StructField("report_date", TimestampType(), True),
                     StructField("call_date", TimestampType(), True),
                     StructField("offense_date", TimestampType(), True),
                     StructField("call_time", StringType(), True),
                     StructField("call_date_time", TimestampType(), True),
                     StructField("disposition", StringType(), True),
                     StructField("address", StringType(), True),
                     StructField("city", StringType(), True),
                     StructField("state", StringType(), True),
                     StructField("agency_id", StringType(), True),
                     StructField("address_type", StringType(), True),
                     StructField("common_location", StringType(), True)])


def df_output(df, table_name, console=True, csv=False, append=True, outputmode="append"):
    # Function that will print the dataframe
    df.printSchema()
    if console == True:
        if append == True:
            query_df= df \
                .writeStream \
                .outputMode(outputmode) \
                .format("console") \
                .option("truncate", "false") \
                .start()
        elif append == False:
            query_df= df \
                .writeStream \
                .outputMode(outputmode) \
                .format("console") \
                .start()
    elif csv == True:
        query_df= df \
            .writeStream \
            .outputMode(outputmode) \
            .option("path", f"/csv/{table_name}.csv") \
            .option("checkpointLocation", "/csv/checkpoint/") \
            .start()
            #.format("console") \
    query_df.awaitTermination()
    
    
def run_spark_job(spark):

    # TODO Create Spark Configuration
    # Create Spark configurations with max offset of 200 per trigger
    # set up correct bootstrap server and port
    df = spark \
        .readStream \
        .option("group.id", "05")\
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "05_sf_crime_data") \
        .option("startingOffsets", "earliest") \
        .option("fetchOffset.retryIntervalMs", 5) \
        .option("maxOffsetsPerTrigger", 200) \
        .load()
    
    # Show schema for the incoming resources for checks
    df.printSchema()
    #df_output(df, df)
    
    # TODO extract the correct column from the kafka input resources
    # Take only value and convert it to String
    kafka_df = df.selectExpr("CAST(value AS STRING)")
    #df_output(kafka_df, kafka_df)
    
    service_table = kafka_df\
                    .select(psf.from_json(psf.col('value'), schema).alias("DF"))\
                    .select("DF.*")
    #df_output(service_table, service_table)
    
    # TODO select original_crime_type_name and disposition
    distinct_table = service_table\
                    .select("original_crime_type_name",
                            "call_date_time",
                           "disposition")\
                    .withWatermark("call_date_time", "1 hour")
    #df_output(distinct_table, distinct_table)
    
    # count the number of original crime type
    agg_df = distinct_table \
            .groupBy("original_crime_type_name", psf.window("call_date_time", "1 hour"), "disposition") \
            .count() 
    
    # TODO Q1. Submit a screen shot of a batch ingestion of the aggregation
    # TODO write output stream
    agg_df.printSchema()
    query = agg_df \
            .writeStream \
            .outputMode("complete") \
            .format("console") \
            .option("truncate", "false") \
            .start()

    # TODO attach a ProgressReporter
    query.awaitTermination()
    
    # TODO get the right radio code json path
    radio_code_json_filepath = "/home/workspace/radio_code.json"
    radio_code_df = spark.read.json(radio_code_json_filepath, multiLine=True)
    #print("-------------------------------------")
    #print(type(radio_code_df))
    #radio_code_df.show(20)
    #time.sleep(1)
    
    # clean up your data so that the column names match on radio_code_df and agg_df
    # we will want to join on the disposition code

    # TODO rename disposition_code column to disposition
    radio_code_df = radio_code_df.withColumnRenamed("disposition_code", "disposition")
    
    # TODO join on disposition column
    join_query = agg_df\
            .join(radio_code_df, "disposition") \
            .writeStream \
            .trigger(processingTime="2 seconds") \
            .outputMode("complete") \
            .format("console") \
            .option("truncate", "false") \
            .start()
        
    join_query.awaitTermination()

    
if __name__ == "__main__":
    logger = logging.getLogger(__name__)

    # TODO Create Spark in Standalone mode
    spark = SparkSession \
        .builder \
        .master("local[*]") \
        .appName("KafkaSparkStructuredStreaming") \
        .config("spark.ui.port",3000) \
        .getOrCreate()
    spark.sparkContext.setLogLevel("INFO")

    logger.info("Spark started")

    run_spark_job(spark)

    spark.stop()
