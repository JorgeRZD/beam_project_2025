from google.cloud import pubsub_v1
import time
import os
from dotenv import load_dotenv

load_dotenv()
path_service_account = (
    "C:/Users/JRZR/Desktop/quick-processor-468404-r8-88b193781db2.json"
)

if __name__ == "__main__":
    # Replace 'my-service-account-path' with your service account path
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = path_service_account

    # Replace 'my-subscription' with your subscription id
    subscription_path = (
        "projects/quick-processor-468404-r8/subscriptions/Test-topic-2-sub"
    )

    subscriber = pubsub_v1.SubscriberClient()

    def callback(message):
        print(("Received message: {}".format(message)))
        message.ack()

    subscriber.subscribe(subscription_path, callback=callback)

    while True:
        time.sleep(60)
