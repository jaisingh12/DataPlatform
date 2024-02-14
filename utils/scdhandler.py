from utils.logger import Logger
from utils.spark_session import SparkSessionManager
from pyspark.sql.functions import concat_ws, lit, col
import json


class SCDHandler:
    def __init__(self):
        self.spark = SparkSessionManager(self.__class__.__name__).create_session()
        self.logger = Logger(self.__class__.__name__)

    def scd_1(self, source_df, target_df, join_keys, metadata_cols=['date_id']):
        # check if all the columns from target df is present in source df
        cols_missing = set([cols for cols in target_df.columns if cols not in source_df.columns]) - set(metadata_cols)
        if cols_missing:
            raise Exception(f"Cols missing in source dataframe: {cols_missing}")

        join_cond = [source_df[join_key] == target_df[join_key] for join_key in join_keys]
        base_df = target_df.join(source_df, join_cond, 'full')
        base_df.show()
