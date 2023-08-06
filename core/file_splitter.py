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
    
    def _get_group_chunks(self, dataframe_grouped: DataFrameGroupBy)-> List[List[str]]:
        rows_added = 0
        chuncked_groups = []
        groups = []        
        for group_name, group_data in dataframe_grouped:
            groups.append(group_name[0])
            rows_added += len(group_data)
            if rows_added > self.minChunkSize:
                chuncked_groups.append(groups)
                rows_added = 0
                groups = []
        if len(groups) > 0:
            chuncked_groups.append(groups)
        return chuncked_groups

    def generate_chunks(self)->pd.DataFrame:
        df = self.reader.readDataFrame()
        grouped = df.groupby(self.partition_key, sort=False)
        chunked_groups = self._get_group_chunks(grouped)
        total_chunks = len(chunked_groups)
        print(f'Total File Chunks = {total_chunks}')
        for chunk_sequence, groups in enumerate(chunked_groups):
            dataframe = pd.DataFrame()
            for  group_name in groups:
                dataframe = pd.concat([dataframe, grouped.get_group(group_name)], ignore_index=True)
            yield chunk_sequence+1, total_chunks, dataframe
         

        