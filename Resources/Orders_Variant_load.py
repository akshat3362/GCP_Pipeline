import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions
from apache_beam.io.gcp.bigquery import WriteToBigQuery
import json
import logging

def to_json_format(content):
    """Prepare the entire content as a JSON object to be inserted into BigQuery."""
    try:
        # Parse the whole content (which is a single JSON object) into a dictionary.
        json_object = json.loads(content)  # This will raise an error if the JSON is invalid
        return {'json_data': json.dumps(json_object)}  # Store the JSON object as a string
    except json.JSONDecodeError as e:
        # Log the invalid JSON for debugging and return None to skip this entry.
        print(f"Error decoding JSON: {e} - Content: {content}")
        return None 

def run():
    pipeline_options = PipelineOptions()
    google_cloud_options = pipeline_options.view_as(GoogleCloudOptions)
    google_cloud_options.project = 'keen-dolphin-436707-b9'
    google_cloud_options.region = 'us-central1'
    google_cloud_options.job_name = 'Orders_load_variant'
    google_cloud_options.staging_location = 'gs://practicegcp69/Staging/'
    google_cloud_options.temp_location = 'gs://practicegcp69/Temp/'
    pipeline_options.view_as(StandardOptions).runner = 'DataflowRunner'  # Use 'DataflowRunner' for actual pipeline runs
    input = 'gs://practicegcp69/Json_input/*.json'
    output = 'keen-dolphin-436707-b9.EDM_CONFIRMED_PRD.Orders'

    p = beam.Pipeline(options=pipeline_options)

    (p
     | "Take input" >> beam.io.ReadFromText(input)
     | "Group into Single String" >> beam.CombineGlobally(lambda lines: ''.join(lines))  # Join all lines into a single string
     | "Parse to JSON" >> beam.Map(to_json_format)  # Convert each line to a raw JSON string
     | "Filter valid JSON" >> beam.Filter(lambda x: x is not None)  # Filter out invalid JSON
     | "Write to BQ" >> beam.io.Write(
            beam.io.WriteToBigQuery(
                output,
                schema='json_data:JSON',  # Ensure the column in BigQuery is of type JSON
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                method='STREAMING_INSERTS'
            )
        )
    )
    p.run().wait_until_finish()

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
