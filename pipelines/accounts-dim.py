from utils.spark_session import SparkSessionManager
from utils.logger import Logger
from utils.scdhandler import SCDHandler
import json

import utils.read as ut


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
    source_df.show(truncate=False)

    target_df = ut.readfunc().read(target_path)
    target_df.show(truncate=False)

    SCDHandler().scd_1(source_df, target_df, join_keys)
