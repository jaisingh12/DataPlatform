from utils.logger import Logger
from utils.spark_session import SparkSessionManager


class readfunc:
    def __init__(self):
        self.spark = SparkSessionManager(self.__class__.__name__).create_session()
        self.logger = Logger(self.__class__.__name__)

    def read(self, path):
        df = self.spark.read.parquet(path)
        return df
