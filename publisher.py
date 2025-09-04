import os
import time
from google.cloud import pubsub_v1
from dotenv import load_dotenv

if __name__ == "__main__":
    # Replace 'my-project' with your project id
    project = "beam-project-2025"

    # Replace 'my-topic' with your pubsub topic
    pubsub_topic = "projects/quick-processor-468404-r8/topics/Test-topic"

    # Replace 'my-service-account-path' with your service account path
    load_dotenv()
    path_service_account = os.getenv("api_key_loc")

    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = path_service_account

    # Replace 'my-input-file-path' with your input file path
    input_file = r"C:/Users/JRZR/Documents/python_scripts/beam_project_2025/counts.csv"

    # create publisher
    publisher = pubsub_v1.PublisherClient()

    with open(input_file, "rb") as ifp:
        # skip header
        header = ifp.readline()

        # loop over each record
        for line in ifp:
            event_data = line  # entire line of input CSV is the message
            print("Publishing {0} to {1}".format(event_data, pubsub_topic))
            publisher.publish(pubsub_topic, event_data)
            time.sleep(4)
