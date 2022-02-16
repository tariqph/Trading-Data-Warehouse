import argparse
import time

from google.cloud import pubsub_v1
from concurrent import futures
import yaml
import os

# Reading config from yaml file
config_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..","..","config.yml"))
try: 
    with open (config_path, 'r') as file:
    	config = yaml.safe_load(file)
except Exception as e:
    print('Error reading the config file')

key_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..","..",config['gcloud']['pubsub_key']))
# Resolve the publish future in a separate thread.
def callback(future: pubsub_v1.publisher.futures.Future) -> None:
    message_id = future.result()
    # print(message_id)


# Example input for publishing
text = b'{"instrument_token": 13720834, "last_price": 17731.3, "volume_traded": 3970700, "buy_quantity": "212850", "sell_quantity": "426100", "oi": 10085850, "exchange_timestamp": "2021-11-18 12:07:07", "open": 17890.0, "high": 17944.0, "low": 17709.0, "close": 17909.15}'

# Batch setting for pub/sub publish
batch_settings = pubsub_v1.types.BatchSettings(
    max_messages = int(config['gcloud']['pubsub_batch_settings']['max_messages']),  # default 100
    max_bytes= int(config['gcloud']['pubsub_batch_settings']['max_bytes']),  # default 1 MiB
    max_latency= int(config['gcloud']['pubsub_batch_settings']['max_latency']),  # default 10 ms
)

# Initialize a Publisher client.
client = pubsub_v1.PublisherClient.from_service_account_json(key_path,batch_settings)

publish_futures = []
def pub(data,project_id: str, topic_id: str) -> None:
    """Publishes a message to a Pub/Sub topic."""
    
    # Create a fully qualified identifier of form `projects/{project_id}/topics/{topic_id}`
    topic_path = client.topic_path(project_id, topic_id)

    # Data sent to Cloud Pub/Sub must be a bytestring.
    # data = json.dumps(text)
    # data = bytes(data, 'utf-8')
    # data = text

    # When you publish a message, the client returns a future.
    publish_future = client.publish(topic_path, data)
    publish_future.add_done_callback(callback)
    publish_futures.append(publish_future)
 

if __name__ == "__main__":
    # parser = argparse.ArgumentParser(
    #     description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter,
    # )
    # parser.add_argument("project_id", help="Google Cloud project ID")
    # parser.add_argument("topic_id", help="Pub/Sub topic ID")

    # args = parser.parse_args()
    # print(args)
    count = 0
    while True:
        # print(config['gcloud']['pubsub_batch_settings']['max_messages'])
        # print(type(config['gcloud']['pubsub_batch_settings']['max_messages']))
        
        pub(text,"zerodha-data-collection", "ticker_data_new")
        time.sleep(1)
        count += 1
        print(count)
