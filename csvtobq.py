import apache_beam as beam
import argparse
from apache_beam.options.pipeline_options import PipelineOptions
from sys import argv

PROJECT_ID = 'bamboo-bulwark-319114'
SCHEMA = 'Supplier:STRING,SupplierID:INTEGER,SupplierPartyId:INTEGER,SupplierNumber:INTEGER'

#def discard_incomplete(data):
    #"""Filters out records that don't have an information."""
    #return len(data['SupplierID']) > 0 and len(data['SupplierPartyId']) > 0 and len(data['name']) > 0 and len(data['style']) > 0


#def convert_types(data):
    #"""Converts string values to their appropriate type."""
    #data['SupplierID'] = float(data['SupplierID']) if 'SupplierID' in data else None
    #data['SupplierPartyId'] = int(data['SupplierPartyId']) if 'SupplierPartyId' in data else None
    #data['name'] = str(data['name']) if 'name' in data else None
    #data['style'] = str(data['style']) if 'style' in data else None
    #data['ounces'] = float(data['ounces']) if 'ounces' in data else None
    #return data

#def del_unwanted_cols(data):
    #"""Delete the unwanted columns"""
    #del data['ibu']
    #del data['brewery_SupplierPartyId']
    #return data

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    known_args = parser.parse_known_args(argv)

    p = beam.Pipeline(options=PipelineOptions())

    (p | 'ReadData' >> beam.io.ReadFromText('gs://cst-i094/Mycsv.csv', skip_header_lines =1)
       | 'SplitData' >> beam.Map(lambda x: x.split(','))
       | 'FormatToDict' >> beam.Map(lambda x: {"Supplier": x[0], "SupplierID": x[1], "SupplierPartyId": x[2], "SupplierNumber": x[3]}) 
       #| 'DeleteIncompleteData' >> beam.Filter(discard_incomplete)
       #| 'ChangeDataType' >> beam.Map(convert_types)
       #| 'DeleteUnwantedData' >> beam.Map(del_unwanted_cols)
       | 'WriteToBigQuery' >> beam.io.WriteToBigQuery(
           '{0}:CST_I094_Sample_Dataset.DM_SUPPLIER'.format(PROJECT_ID),
           schema=SCHEMA,
           write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND))
    result = p.run()