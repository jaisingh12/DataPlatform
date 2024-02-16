from pyspark.sql import DataFrame
from pyspark.sql.functions import concat_ws, md5, col, current_date, lit

from utils.logger import Logger
from utils.spark_session import SparkSessionManager


class SCDHandler:
    def __init__(self):
        self.spark = SparkSessionManager(self.__class__.__name__).create_session()
        self.logger = Logger(self.__class__.__name__)

    def check_columns_presence(self, source_df, target_df, metadata_cols):
        """
        Check if all columns from the target DataFrame are present in the source DataFrame.

        Args:
            source_df (pyspark.sql.DataFrame): Source DataFrame.
            target_df (pyspark.sql.DataFrame): Target DataFrame.

        Raises:
            Exception: If columns are missing in the source DataFrame.

        Returns:
            None
        """
        cols_missing = set([cols for cols in target_df.columns if cols not in source_df.columns]) - set(metadata_cols)
        if cols_missing:
            raise Exception(f"Cols missing in source DataFrame: {cols_missing}")

    def apply_hash_and_alias(self, source_df, target_df, metadata_cols) -> ([DataFrame, DataFrame]):
        """
        Apply hash calculation and alias to source and target DataFrames.

        Args:
            source_df (pyspark.sql.DataFrame): Source DataFrame.
            target_df (pyspark.sql.DataFrame): Target DataFrame.
            metadata_cols (list): List of metadata columns to exclude from hash calculation.

        Returns:
            tuple: Tuple containing aliased source DataFrame and aliased target DataFrame.
        """
        # Extract columns from target DataFrame excluding metadata columns
        tgt_cols = [x for x in target_df.columns if x not in metadata_cols]

        # Calculate hash expression
        hash_expr = md5(concat_ws("|", *[col(c) for c in tgt_cols]))

        # Apply hash calculation and alias to source and target DataFrames
        source_df = source_df.withColumn("hash_value", hash_expr).alias("source_df")
        target_df = target_df.withColumn("hash_value", hash_expr).alias("target_df")

        return source_df, target_df

    def scd_1(self, source_df, target_df, join_keys, metadata_cols=None) -> DataFrame:
        if metadata_cols is None:
            metadata_cols = []
        tgt_cols = [x for x in target_df.columns]
        self.check_columns_presence(source_df, target_df, metadata_cols)
        source_df, target_df = self.apply_hash_and_alias(source_df, target_df, metadata_cols)

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

    def scd_2(self, source_df, target_df, join_keys, metadata_cols=None) -> DataFrame:
        if metadata_cols is None:
            metadata_cols = ['eff_start_date', 'eff_end_date', 'flag']
        tgt_cols = [x for x in target_df.columns]
        self.check_columns_presence(source_df, target_df, metadata_cols)
        # Apply hash calculation and alias
        source_df, target_df = self.apply_hash_and_alias(source_df, target_df, metadata_cols)

        # Identify new records
        join_cond = [source_df[join_key] == target_df[join_key] for join_key in join_keys]
        new_df = source_df.join(target_df, join_cond, 'left_anti')

        base_df = target_df.join(source_df, join_cond, 'left')

        # Filter unchanged records or same records
        unchanged_filter_expr = " AND ".join([f"source_df.{key} IS NULL" for key in join_keys])
        unchanged_df = base_df.filter(f"({unchanged_filter_expr}) OR "
                                      f"(source_df.hash_value = target_df.hash_value)") \
            .select("target_df.*")

        # identify updated records
        delta_filter_expr = " and ".join([f"source_df.{key} IS NOT NULL" for key in join_keys])
        updated_df = base_df.filter(f"{delta_filter_expr} AND "
                                    f"source_df.hash_value != target_df.hash_value")

        # pick updated records from source_df for new entry
        updated_new_df = updated_df.select("source_df.*")

        # pick updated records from target_df for obsolete entry
        obsolete_df = updated_df.select("target_df.*") \
            .withColumn("eff_end_date", current_date()) \
            .withColumn("flag", lit(0))

        # union : new & updated records and add scd2 meta-deta
        delta_df = new_df.union(updated_new_df) \
            .withColumn("eff_start_date", current_date()) \
            .withColumn("eff_end_date", lit(None)) \
            .withColumn("flag", lit(1))

        # union all datasets : delta_df + obsolete_df + unchanged_df
        result_df = unchanged_df.select(tgt_cols). \
            unionByName(delta_df.select(tgt_cols)). \
            unionByName(obsolete_df.select(tgt_cols))

        return result_df
