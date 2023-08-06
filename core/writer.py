import boto3
from io import BytesIO
import pandas as pd
from abc import ABC, abstractmethod
from .reader import DataFrameMetaData
import os

class DataFrameWriter(ABC):
    @abstractmethod
    def write(self, dataframe:pd.DataFrame, metadata: DataFrameMetaData)->None:
        pass

class LocalDataFrameToExcelWriter(DataFrameWriter):
    def __init__(self, basepath) -> None:
        self.basepath = basepath
        super().__init__()
    
    def write(self, dataframe:pd.DataFrame, metadata: DataFrameMetaData)->None:
        if metadata.name:
            dataframe.to_excel(os.path.join(self.basepath, metadata.name), index=False)

class S3DataFrameToExcelWriter(DataFrameWriter):
    def __init__(self, bucket:str) -> None:
        self.s3 =  boto3.client('s3')
        self.bucket = bucket
    
    def get_file_buffer(self, dataframe:pd.DataFrame):
        buffer = BytesIO()
        dataframe.to_excel(buffer, index=False)
        buffer.seek(0)
        return buffer
    
    def write(self, dataframe:pd.DataFrame, metadata: DataFrameMetaData)->None:
        if metadata.name:
            metadata = metadata.extra_data
            key = metadata.name
            self.s3.put_object(Body=self.get_file_buffer(dataframe), Bucket=self.bucket, Key=key, Metadata=metadata)
        else:
            raise KeyError("Object Key/ File name not specified in the parameter")
    
