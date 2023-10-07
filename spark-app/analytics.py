from pyspark.sql import DataFrame
from pyspark.sql.functions import count
from pyignite import Client

class AnalyticsProcessor:
    def __init__(self, ignite_config, ignite_cache_name):
        self.ignite_config = ignite_config
        self.ignite_cache_name = ignite_cache_name
        self.ignite_client = Client()

    def process_data(self, stream_data: DataFrame):
        # Connect to Ignite
        self.ignite_client.connect(self.ignite_config["host"], self.ignite_config["port"])

        # Get or create Ignite cache
        ignite_cache = self.ignite_client.get_or_create_cache(self.ignite_cache_name)

        # Process incoming data and calculate purchase counts for each user
        
        purchase_counts = stream_data.filter(stream_data["data.action"] == "purchase") \
            .groupBy("data.user") \
            .agg(count("data.action").alias("purchase_count"))

        for row in purchase_counts.collect():
            user = row["user"]
            count_val = row["purchase_count"]
            ignite_cache.put(user, count_val)

        # Disconnect from Ignite
        self.ignite_client.close()

