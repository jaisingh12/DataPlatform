from utils.spark_session import SparkSessionManager
from utils.logger import Logger
from utils.scdhandler import SCDHandler
import json

import utils.read as ut
from pyspark.sql.functions import *


class AccountDim:
    def __init__(self):
        self.spark = SparkSessionManager(self.__class__.__name__).create_session()
        self.logger = Logger(self.__class__.__name__)


if __name__ == "__main__":
    with open("../config/accounts-dim.json") as f:
        config = json.load(f)

    source_path = config.get("source_table")
    target_path = config.get("target_table")
    join_keys = config.get("join_keys")

    # to be replaced with hive external tables
    source_df = ut.readfunc().read(source_path)

    # make source df & target df in sync before scd1
    pattern = r"accounts_(\d{4})(\d{2})(\d{2})(\d{6})\.csv"
    source_df = source_df.withColumn("date_id", regexp_replace("filename", pattern, "$1-$2-$3")) \
        .drop("year", "month", "day", "filename")
    source_df.show(truncate=False)

    target_df = ut.readfunc().read(target_path)
    target_df.show(truncate=False)

    result_df = SCDHandler().scd_1(source_df, target_df, join_keys, ["date_id", "customer_id"])
    result_df.show(truncate=False)
