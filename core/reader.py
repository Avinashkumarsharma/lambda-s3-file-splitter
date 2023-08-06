from abc import ABC, abstractmethod
import pandas as pd
import boto3
from pathlib import Path

class DataFrameMetaData(object):
    def __init__(self, name:str, extra_data = {}) -> None:
        self.name = name
        self.extra_data = extra_data

class DataFrameReader(ABC):
    
    @abstractmethod
    def readDataFrame(self)->pd.DataFrame:
        pass

    @property
    def metadata(self) ->DataFrameMetaData:
        pass

class ExcelLocalFileReader(DataFrameReader):
    
    def __init__(self, file_path) -> None:
        self.file_path = file_path
    
    def readDataFrame(self)->pd.DataFrame:
        excel_file = pd.read_excel(self.file_path)
        return excel_file
    
    @property
    def metadata(self):
        return DataFrameMetaData(Path(self.file_path).name)
    
class ExcelAmazonS3Reader(DataFrameReader):
    def __init__(self, bucket, key) -> None:
        self.bucket = bucket
        self.key = key
        self._raw_s3_object = None
        self.s3 = boto3.client('s3')
        super().__init__()
    
    def readDataFrame(self)->pd.DataFrame:
        try:
            response = self.s3.get_object(Bucket=self.bucket, Key=self.key)
            print("CONTENT TYPE: " + response['ContentType'])
            excel_file = pd.read_excel(response['Body'].read())
            self._raw_s3_object = response
            return excel_file
        except Exception as e:
            print(e)
            raise e
    
    @property
    def metadata(self):
        filename = Path(self.key).name
        response = self.s3.head_object(Bucket=self.bucket, Key=self.key)
        metadata = response['Metadata']
        return DataFrameMetaData(filename, metadata)

