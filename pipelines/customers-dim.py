from utils.spark_session import SparkSessionManager
from utils.logger import Logger
from utils.scdhandler import SCDHandler
from ingestion.DataWriter import write_output

from utils.read import readHandler
from pyspark.sql.functions import *


class CustomersDim:
    def __init__(self):
        self.spark = SparkSessionManager(self.__class__.__name__).create_session()
        self.logger = Logger(self.__class__.__name__)
        self.config = readHandler().read_json("../config/customers-dim.json")

    def etl(self):
        source_path = self.config.get("source_table")
        target_path = self.config.get("target_table")
        join_keys = self.config.get("join_keys")

        # to be replaced with hive external tables
        target_df = readHandler().read_parquet(target_path)
        target_df.show(truncate=False)

        source_df = readHandler().read_parquet(source_path)

        # make source df & target df in sync before scd1
        pattern = r"customers_(\d{4})(\d{2})(\d{2})(\d{6})\.csv"
        source_df = source_df.withColumn("date_id", regexp_replace("filename", pattern, "$1-$2-$3")) \
            .drop("year", "month", "day", "filename")
        source_df.show(truncate=False)

        result_df = SCDHandler().scd_2(source_df, target_df, join_keys)
        result_df.cache()
        result_df.show(truncate=False)

        write_output(result_df, target_path, "parquet", "overwrite", "date_id")
        self.logger.info(f"SCD-2 completed, data loaded to path {target_path}")


if __name__ == '__main__':
    CustomersDim().etl()
