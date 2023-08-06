from abc import ABC, abstractmethod
from core.reader import DataFrameReader
from core.writer import DataFrameWriter
from typing import List, Union
from pandas.core.groupby.generic import DataFrameGroupBy
import pandas as pd

class FileSplitter(ABC):
    def __init__(self, minChunkSize) -> None:
        self.minChunkSize = minChunkSize 
        super().__init__()
    
    @abstractmethod
    def generate_chunks(self)->pd.DataFrame:
        pass

class ExcelBasicFileSpliter(FileSplitter):
    def __init__(self, reader: DataFrameReader, partition_key: Union[str, List[str]], minRows) -> None:
        self.reader = reader
        self.partition_key = partition_key
        super().__init__(minRows)
    
    def generate_chunks(self)->pd.DataFrame:
        print(f'Reading file into memory filename = {self.reader.metadata.name}')
        df = self.reader.readDataFrame()
        print(f'Grouping on key {str(self.partition_key)}')
        grouped = df.groupby(self.partition_key, sort=False, as_index=False)
        df = grouped.apply(lambda group: group).reset_index(drop=True)
        # Calculate the total number of chunks based on the chunk size and DataFrame size
        num_chunks = len(df) // self.minChunkSize + int(len(df) % self.minChunkSize != 0)
        for i in range(num_chunks):
            dataframe = df.iloc[i * self.minChunkSize: (i + 1) * self.minChunkSize]
            yield i+1, num_chunks, dataframe
         