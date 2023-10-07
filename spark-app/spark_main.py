from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType
from kafka_streaming import KafkaStreamProcessor
from analytics import AnalyticsProcessor
import argparse


class SparkApp:
    def __init__(self):
        pass

    def create_spark_session(self, app_name):
        spark = SparkSession.builder \
            .appName(app_name) \
            .getOrCreate()
        
        return spark

    def run(self):
        spark = self.create_spark_session("VortexApp")

        # Define the Kafka configuration and topic

        parser = argparse.ArgumentParser(description='Receive events from kafka topic stream for spark app')
        parser.add_argument('--bootstrap-servers', required=False, default="localhost:9092", help='Kafka bootstrap servers (e.g., localhost:9092)')
        parser.add_argument('--topic', required=False, default="vortex-events", help='Kafka topic to produce events to')
        args = parser.parse_args()

        kafka_stream_processor = KafkaStreamProcessor(spark,
                                                    args.bootstrap_servers,
                                                    args.topic
                                                    )
        
        kafka_stream_data = kafka_stream_processor.create_stream()

        # Define the Ignite configuration and cache name
        ignite_config = {
            "host": "localhost",
            "port": 10800
        }
        ignite_cache_name = "user_actions"

        # create analytics processor
        analytics_processor = AnalyticsProcessor(ignite_config, ignite_cache_name)

        # Process incoming data and store it in ignite and write to console live
        # Makes use of both spark's capabilities to write processed data stream
        # and ignite's capabilities to store incoming data stream
        kafka_stream_data.writeStream \
            .outputMode("append") \
            .format("console") \
            .foreachBatch(lambda batch_df, batch_id: self.process_and_write(batch_df, batch_id, analytics_processor)) \
            .start() \
            .awaitTermination()

    def process_and_write(self, batch_df, batch_id, analytics_processor):
        analytics_processor.process_data(batch_df)


if __name__ == "__main__":
    spark_app = SparkApp()
    spark_app.run()