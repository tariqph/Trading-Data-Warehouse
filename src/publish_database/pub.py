
import argparse
import time

from google.cloud import pubsub_v1
from concurrent import futures


key_path = "G:\DS - Competitions and projects\Zerodha\gcloud\key.json"
# Resolve the publish future in a separate thread.
def callback(future: pubsub_v1.publisher.futures.Future) -> None:
    message_id = future.result()
    # print(message_id)


# Example input for publishing
text = b'{"instrument_token": 13720834, "last_price": 17731.3, "volume": 3970700, "buy_quantity": "212850", "sell_quantity": "426100", "oi": 10085850, "timestamp": "2021-11-18 12:07:07", "open": 17890.0, "high": 17944.0, "low": 17709.0, "close": 17909.15}'

# Batch setting for pub/sub publish
batch_settings = pubsub_v1.types.BatchSettings(
    max_messages=1000,  # default 100
    max_bytes=100000,  # default 1 MiB
    max_latency=2,  # default 10 ms
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
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("project_id", help="Google Cloud project ID")
    parser.add_argument("topic_id", help="Pub/Sub topic ID")

    args = parser.parse_args()
    print(args)
    while True:
        pub("any",args.project_id, args.topic_id)
        time.sleep(0.1)
