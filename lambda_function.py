import os
import pathlib
import urllib.parse
from typing import List, Union

from core.file_splitter import ExcelBasicFileSpliter
from core.reader import (DataFrameMetaData, DataFrameReader,
                         ExcelAmazonS3Reader, ExcelLocalFileReader)
from core.writer import (LocalDataFrameToExcelWriter,
                         S3DataFrameToExcelWriter)

print('Loading function')

def get_partion_key(datafile_reader: DataFrameReader)->Union[str, List[str]]:
    '''
    Ideally this should read the actual mapping that needs to be used from the datafile metadata and then return accordingly.
    Hardcoding right now
    '''
    return ['Invoice Number']

def get_write_s3_bucket():
    '''
    Reading the bucket name from the environment variable.
    '''
    return os.environ['WRITE_BUCKET']

def get_s3_key_prefix():
    return os.environ.get('CHUNK_KEY_PREFIX', '')

def get_min_rows_per_chunk():
    return int(os.environ.get('MIN_ROWS_PER_CHUNK', 40000))

def chunk_file_name(name_prefix, chunk_number):
    return f'{name_prefix}_{chunk_number}.xlsx'

'''
This is the meta data that we want to write to each chunk of file.
Aws expects the values to be string .
'''
def get_chunk_file_s3_metadata(chunk: DataFrameReader, total_chunks):
    metadata = chunk.metadata.extra_data
    metadata['total_chunk'] = str(total_chunks)
    return metadata

def lambda_handler(event, context):
    #print("Received event: " + json.dumps(event, indent=2))

    # Get the object from the event and show its content type
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    excel_file = ExcelAmazonS3Reader(bucket=bucket, key=key)
    splitter = ExcelBasicFileSpliter(excel_file, partition_key=get_partion_key(excel_file), minRows=get_min_rows_per_chunk())
    splitted_chunks = splitter.generate_chunks()

    for chunk_number, total_chunks, chunk_dataframe in splitted_chunks:
        bucket = get_write_s3_bucket()
        key_path = pathlib.Path(key)
        file_name = chunk_file_name(key_path.stem, chunk_number)
        file_key = f'{os.path.join(get_s3_key_prefix(), file_name)}'
        s3writter = S3DataFrameToExcelWriter(bucket=bucket)
        print(f'Writing Chunk {chunk_number}/{total_chunks} for File = {file_key}')
        s3writter.write(chunk_dataframe, DataFrameMetaData(name=file_key, extra_data=get_chunk_file_s3_metadata(excel_file, total_chunks)))

'''
This was made to write locally all the splitted excel file and test locally before deploying
'''
def test_local_handler():
    excel_file = ExcelLocalFileReader('./build/TaxInvoice.xlsx') # make sure you give the correct path of the local file
    writer = LocalDataFrameToExcelWriter('./build') # make sure you have this folder locally
    splitter = ExcelBasicFileSpliter(excel_file, partition_key=['Invoice Number'], minRows=300)
    splitted_chuncks = splitter.generate_chunks()
    for chunk_number, total_chunks, chunk_dataframe in splitted_chuncks:
        print(f'Writing Chunk {chunk_number}/{total_chunks} for File = TaxInvoice')
        writer.write(chunk_dataframe, DataFrameMetaData(chunk_file_name('TaxInvoice_Generated', chunk_number), {}))


def test_lambda_handler():
    with open('./util/s3_event.json') as s3_event_json:
        event = json.load(s3_event_json)
        lambda_handler(event, None)

if __name__ =='__main__':
    # test_local_handler()
    import os
    import json
    from dotenv import load_dotenv
    # load environment variable from .env file for local testing.
    # These will be set on Lambda in Aws
    load_dotenv()
    
    # Comment out below for Testing Locally splitting and writing
    # test_local_handler()

    #Comment out below for Testing with uploading the splitted file to s3 locally
    # test_lambda_handler()