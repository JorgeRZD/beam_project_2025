import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
import os
from apache_beam import window
from dotenv import load_dotenv

load_dotenv()
path_service_account = (
    "C:/Users/JRZR/Desktop/quick-processor-468404-r8-88b193781db2.json"
)
# Replace 'my-service-account-path' with your service account path
print("Service account file : ", path_service_account)
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = path_service_account

# Replace 'my-input-subscription' with your input subscription id
input_subscription = "projects/quick-processor-468404-r8/subscriptions/test-sub-1"

# Replace 'my-output-subscription' with your output subscription id
output_topic = "projects/quick-processor-468404-r8/topics/Test-topic-2"

options = PipelineOptions()
options.view_as(StandardOptions).streaming = True

p = beam.Pipeline(options=options)


output_file = "outputs/part"

pubsub_data = (
    p
    | "Read from pub sub" >> beam.io.ReadFromPubSub(subscription=input_subscription)
    | "Write to pus sub" >> beam.io.WriteToPubSub(output_topic)
)

result = p.run()
result.wait_until_finish()
