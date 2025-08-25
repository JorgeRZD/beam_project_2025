import os
import time
from google.cloud import pubsub_v1

if __name__ == "__main__":
    # Replace 'my-project' with your project id
    project = "beam-project-2025"

    # Replace 'my-topic' with your pubsub topic
    pubsub_topic = "projects/quick-processor-468404-r8/topics/Test-topic"

    # Replace 'my-service-account-path' with your service account path
    path_service_account = (
        r"C:\Users\JRZR\Desktop\lustrous-strand-468405-u8-5dfd46f025f4.json"
    )
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = path_service_account

    # Replace 'my-input-file-path' with your input file path
    input_file = r"C:\Users\JRZR\Documents\Python Scripts\beam_project_2025\counts.csv"

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
            time.sleep(2)
