from pyspark.sql import SparkSession
from pyspark.sql.functions import window, col, min, max, first, last, from_json, expr
from pyspark.sql.types import StructType, StringType, DoubleType, LongType, BooleanType

import os


class ticktominstreaming():
    def __init__(self, kafka_url, tick_topic, min_topic) -> None:

        self.kafka_url = kafka_url
        self.tick_topic = tick_topic
        self.min_topic = min_topic


        self.spark = SparkSession \
            .builder \
            .appName("KafkaToSparkStreamingOHLC") \
            .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0') \
            .getOrCreate()

        # 틱 데이터의 스키마 정의
        self.schema = StructType() \
            .add("T", LongType()) \
            .add("s", StringType()) \
            .add("p", StringType()) 
        
    def read_stream(self):
        # Kafka 스트림 읽기 설정
        df = self.spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.kafka_url) \
            .option("subscribe", self.tick_topic) \
            .load()

        # Kafka의 바이너리 메시지를 문자열로 변환하고 JSON으로 파싱하여 스키마 적용
        df = df.selectExpr("CAST(value AS STRING)") \
            .select(from_json(col("value"), self.schema).alias("data")) \
            .select("data.*")

        # 타임스탬프 및 가격 데이터 타입 변환
        df = df.withColumn("timestamp", (col('T') / 1000).cast("timestamp")) \
            .withColumn("price", col("p").cast(DoubleType()))

        return df

    def normalize_timestamp(self, df):
        # 타임스탬프를 5분 간격으로 정규화
        df = df.withColumn("timestamp", 
                        expr("cast((cast(timestamp as long) / 60) * 60 as timestamp)"))
        return df
    
    def aggregate_ohlc(self, df):
        # 지연없이
        ohlc_df = df \
                    .groupBy(window(col("timestamp"), "1 minute")) \
                    .agg(first("price").alias("open"),
                        max("price").alias("high"),
                        min("price").alias("low"),
                        last("price").alias("close"))

        # 1분 단위로 윈도우 생성 및 OHLC 데이터 계산, 10분의 지연을 허용하는 watermark 설정
        # ohlc_df = df.withWatermark("timestamp", "10 minutes") \
        #             .groupBy(window(col("timestamp"), "1 minute")) \
        #             .agg(first("price").alias("open"),
        #                  max("price").alias("high"),
        #                  min("price").alias("low"),
        #                  last("price").alias("close"))
        return ohlc_df

    def stream_to_console(self, ohlc_df):
        query = ohlc_df \
            .writeStream \
            .outputMode("append") \
            .format("console") \
            .start()

        query.awaitTermination()

    def stream_to_kafka(self, ohlc_df):
        query = ohlc_df \
            .selectExpr("to_json(struct(*)) AS value") \
            .writeStream \
            .outputMode("append") \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.kafka_url) \
            .option("topic", self.min_topic) \
            .start()
        
        query.awaitTermination()


if __name__ == '__main__':

    kafka_url = os.getenv('KAFKA_URL', 'default_url')
    tick_topic = 'tttick'
    min_topic = 'ttmin'

    tick_streming = ticktominstreaming(kafka_url, tick_topic, min_topic)
    df_stream = tick_streming.read_stream()
    df_normalized =  tick_streming.normalize_timestamp(df_stream)
    ohlc_df = tick_streming.aggregate_ohlc(df_normalized)
    tick_streming.stream_to_console(ohlc_df)



