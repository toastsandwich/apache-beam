import re
import apache_beam as beam
from apache_beam.options.pipeline_options import (
    PipelineOptions,
    GoogleCloudOptions,
    StandardOptions,
)

# Initialize pipeline options
options = PipelineOptions()


# Class to parse each log entry
class ParseLogFn(beam.DoFn):
    def process(self, element: str):
        log_regex = (
            r'^(\S+) - - \[.*?\] "(\S+) (\/\S*?) \S+" (\d+) \d+ "[^"]*" "[^"]*"$'
        )
        match = re.match(log_regex, element)
        if match:
            ip = match.group(1)
            method = match.group(2)
            uri = match.group(3)
            status = int(match.group(4))
            yield f"{ip} made request to {uri}, method was {method} and status was {status}"
        else:
            # Log or handle lines that don't match the regex
            yield f"Unmatched log line: {element}"


# Set Google Cloud and pipeline options
google_cloud_options = options.view_as(GoogleCloudOptions)
google_cloud_options.project = "lazer-beam-ultra-thick"
google_cloud_options.job_name = "create-update-records"
google_cloud_options.temp_location = "gs://bkt-server-log/temp"
google_cloud_options.region = "us-east1"

options.view_as(StandardOptions).runner = "DataflowRunner"

# Create and run the pipeline
with beam.Pipeline(options=options) as p:
    lines = p | "Read" >> beam.io.ReadFromText("gs://bkt-server-log/access.log")
    logs = lines | "ParseLogs" >> beam.ParDo(ParseLogFn())
    logs | "Write" >> beam.io.WriteToText("gs://bkt-server-log/parsed-logs.txt")
