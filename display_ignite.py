from pyignite import Client

ignite_config = {
    "host": "localhost",
    "port": 10800
}
ignite_cache_name = "user_actions"

ignite_client = Client()
ignite_client.connect(ignite_config["host"], ignite_config["port"])
data_cache = ignite_client.get_or_create_cache(ignite_cache_name)

# get the purchase for each user stored in the cache
for user, purchase_count in data_cache.scan():
    print(f"{user} has {purchase_count} purchases")

ignite_client.close()