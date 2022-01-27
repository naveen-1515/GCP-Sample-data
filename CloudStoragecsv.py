import argparse
import logging
import re
#import os
from google.cloud import bigquery
import requests
import sys
from google.oauth2 import service_account

import os
import pandas as pd
import csv
from google.cloud import storage

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

class DataIngestion:
    """A helper class which contains the logic to translate the file into
    a format BigQuery will accept."""
    
def run(argv=None):
    """The main function which creates the pipeline and runs it."""

    parser = argparse.ArgumentParser()

    # Here we add some specific command line arguments we expect.
    # Specifically we have the input file to read and the output table to write.
    # This is the final stage of the pipeline, where we define the destination
    # of the data. In this case we are writing to BigQuery.
    parser.add_argument(
        '--input',        
        dest='input',
        required=False,
        help='Input file to read. This can be a local file or '
        'a file in a Google Storage Bucket.',
        # This example file contains a total of only 10 lines.
        # Useful for developing on a small set of data.
        #default= values,
        default='gs://cst-i094/Mycsv.csv'
        #type=str
        )

    # This defaults to the lake dataset in your BigQuery project. You'll have
    # to create the lake dataset yourself using this command:
    # bq mk lake
    parser.add_argument('--output',
                        dest='output',
                        required=False,
                        help='Output BQ table to write results to.',
                        default='CST_I094_Sample_Dataset.DM_SUPPLIER')

    # Parse arguments from the command line.
    known_args, pipeline_args = parser.parse_known_args(argv)

    # DataIngestion is a class we built in this script to hold the logic for
    # transforming the file into a BigQuery table.
    data_ingestion = DataIngestion()

    # Initiate the pipeline using the pipeline arguments passed in from the
    # command line. This includes information such as the project ID and
    # where Dataflow should store temp files.
    p = beam.Pipeline(options=PipelineOptions(pipeline_args))

    (p
     # Read the file. This is the source of the pipeline. All further
     # processing starts with lines read from the file. We use the input
     # argument from the command line. We also skip the first line which is a
     # header row.
     | 'Read from a File' >> beam.io.ReadFromText(known_args.input,
                                                  skip_header_lines=1)
     # This stage of the pipeline translates from a CSV file single row
     # input as a string, to a dictionary object consumable by BigQuery.
     # It refers to a function we have written. This function will
     # be run in parallel on different workers using input from the
     # previous stage of the pipeline.
     | 'String To BigQuery Row' >>
     beam.Map(lambda s: data_ingestion.parse_method(s)) |
     'Write to BigQuery' >> beam.io.Write(
         beam.io.gcp.bigquery.WriteToBigQuery(
             # The table name is a required argument for the BigQuery sink.
             # In this case we use the value passed in from the command line.
             known_args.output,
             # Here we use the simplest way of defining a schema:
             # fieldName:fieldType
                        # Creates the table in BigQuery if it does not yet exist.
             create_disposition='CREATE_IF_NEEDED',
             # Deletes all data in the BigQuery table before writing.
             write_disposition='WRITE_TRUNCATE')))
    #p.run()
    p.run().wait_until_finish()
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = r'bamboo-bulwark-319114-c2cf1bc6daac.json'
    client = storage.Client()
    #bucket = client.get_bucket('my-project-1531915255316')  
    bucketName = 'cst-i094'
    fileName = "Mycsv.csv"
    bucket = client.bucket(bucketName)
    blob = bucket.blob(fileName)
    blob.delete()
    print("Blob {} deleted.".format(fileName))

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)    
    run()
    print("Writing to BQ completed")