from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType

class KafkaStreamProcessor:
    def __init__(self, spark, kafka_bootstrap_servers, kafka_topic):
        self.spark = spark
        self.kafka_bootstrap_servers = kafka_bootstrap_servers
        self.kafka_topic = kafka_topic

    def create_stream(self):
        # Define a schema for the Kafka messages
        schema = StructType([
            StructField("user", StringType()),
            StructField("product", StringType()),
            StructField("action", StringType()),
            StructField("timestamp", StringType())
        ])

        # Create a streaming dataframe that reads from Kafka
        kafka_stream_data = self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.kafka_bootstrap_servers) \
            .option("subscribe", self.kafka_topic) \
            .load()
        
        # Parse the value column of the kafka message as json
        kafka_parsed_data = kafka_stream_data.selectExpr(
            "CAST(value AS STRING)") \
            .select(from_json(col("value"), schema).alias("data")
        )

        return kafka_parsed_data