from pyspark.sql import SparkSession
from pyspark.sql.functions import window, col, min, max, first, last, from_json, expr, to_timestamp
from pyspark.sql.types import StructType, StringType, DoubleType, LongType, BooleanType

import os
import concurrent.futures
import logging

class ticktominstreaming():
    def __init__(self, kafka_url, tick_topic, min_topic) -> None:

        self.kafka_url = kafka_url
        self.tick_topic = tick_topic
        self.min_topic = min_topic


        self.spark = SparkSession \
            .builder \
            .appName("KafkaToSparkStreamingOHLC") \
            .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0') \
            .config('spark.sql.streaming.checkpointLocation', '/tmp/checkpoint') \
            .getOrCreate()
        
        self.spark.sparkContext.setLogLevel("WARN")

        # 틱 데이터의 스키마 정의
        self.schema = StructType() \
            .add("현재시간", LongType()) \
            .add("종목코드", StringType()) \
            .add("현재가", StringType()) 
        
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
        df = df.withColumn("timestamp", to_timestamp(col("현재시간").cast("string"), "yyyyMMddHHmmss")) \
            .withColumn("price", col("현재가").cast(DoubleType()))
        
        return df
    
    def aggregate_ohlc(self, df):
        # 지연없이
        ohlc_df = df \
                    .withWatermark("timestamp", "10 seconds") \
                    .groupBy(window(col("timestamp"), "5 minute")) \
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
        try:
            query = ohlc_df \
                .writeStream \
                .outputMode("append") \
                .format("console") \
                .start()

            query.awaitTermination()
        except Exception as e:
            logging.error(f"스트리밍 중 오류 발생: {e}")
            query.stop()

    def stream_to_kafka(self, ohlc_df):
        try:
            query = ohlc_df \
                .selectExpr("CAST(window.start AS STRING) AS key", "to_json(struct(*)) AS value") \
                .writeStream \
                .outputMode("append") \
                .format("kafka") \
                .option("kafka.bootstrap.servers", self.kafka_url) \
                .option("topic", self.min_topic) \
                .start()
            
            query.awaitTermination()
        except Exception as e:
            logging.error(f"Kafka로 스트리밍 중 오류 발생: {e}")
            query.stop()

    def run(self, ohlc_df):
        # ThreadPoolExecutor를 사용하여 두 스트리밍 작업을 병렬로 실행
        with concurrent.futures.ThreadPoolExecutor() as executor:
            executor.submit(self.stream_to_kafka, ohlc_df)
            executor.submit(self.stream_to_console, ohlc_df)

def set_logging(log_level):
    # 로그 생성
    logger = logging.getLogger()
    # 로그 레벨 문자열을 적절한 로깅 상수로 변환
    log_level_constant = getattr(logging, log_level, logging.INFO)
    # 로그의 출력 기준 설정
    logger.setLevel(log_level_constant)
    # log 출력 형식
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    # log를 console에 출력
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)
    # log를 파일에 출력
    #file_handler = logging.FileHandler('GoogleTrendsBot.log')
    #file_handler.setFormatter(formatter)
    #logger.addHandler(file_handler)

if __name__ == '__main__':

    log_level = os.getenv('LOG_LEVEL', 'INFO')
    set_logging(log_level)

    kafka_url = os.getenv('KAFKA_URL', 'default_url')
    tick_topic = 'stock_data_action'
    min_topic = 'tt_min'

    tick_streming = ticktominstreaming(kafka_url, tick_topic, min_topic)
    df_stream = tick_streming.read_stream()
    ohlc_df = tick_streming.aggregate_ohlc(df_stream)
    tick_streming.run(ohlc_df)



