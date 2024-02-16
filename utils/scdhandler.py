from pyspark.sql.functions import concat_ws, md5, col

from utils.logger import Logger
from utils.spark_session import SparkSessionManager


class SCDHandler:
    def __init__(self):
        self.spark = SparkSessionManager(self.__class__.__name__).create_session()
        self.logger = Logger(self.__class__.__name__)

    def scd_1(self, source_df, target_df, join_keys, metadata_cols=[]):
        # check if all the columns from target df is present in source df
        cols_missing = set([cols for cols in target_df.columns if cols not in source_df.columns])
        if cols_missing:
            raise Exception(f"Cols missing in source dataframe: {cols_missing}")

        # Apply hash calculation and alias to source and target DataFrames
        tgt_cols = [x for x in target_df.columns if x not in metadata_cols]
        hash_expr = md5(concat_ws("|", *[col(c) for c in tgt_cols]))
        source_df = source_df.withColumn("hash_value", hash_expr).alias("source_df")
        target_df = target_df.withColumn("hash_value", hash_expr).alias("target_df")

        # Perform full outer join between source and target DataFrames
        join_cond = [source_df[join_key] == target_df[join_key] for join_key in join_keys]
        base_df = target_df.join(source_df, join_cond, 'full')

        # Filter unchanged records or same records
        unchanged_filter_expr = " AND ".join([f"source_df.{key} IS NULL" for key in join_keys])
        unchanged_df = base_df.filter(f"({unchanged_filter_expr}) OR "
                                      f"(source_df.hash_value = target_df.hash_value)") \
            .select("target_df.*")

        # Filter updated records
        delta_filter_expr = " and ".join([f"source_df.{key} IS NOT NULL" for key in join_keys])
        updated_df = base_df.filter(f"{delta_filter_expr} AND "
                                    f"source_df.hash_value != target_df.hash_value") \
            .select("source_df.*")

        # Filter new records
        new_df = base_df.filter(f"{delta_filter_expr} AND target_df.hash_value IS NULL") \
            .select("source_df.*")

        # Combine all dfs into result DataFrame
        result_df = new_df.select(tgt_cols). \
            unionByName(updated_df.select(tgt_cols)). \
            unionByName(unchanged_df.select(tgt_cols))

        return result_df
