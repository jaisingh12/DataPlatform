from ingestion.DataReader import DataReader
from ingestion.DataWriter import DataWriter
from utils.logger import Logger
from utils.spark_session import SparkSessionManager
from pyspark.sql.functions import concat_ws


class BankPipeline:
    def __init__(self) -> None:
        self.logger = Logger(self.__class__.__name__)
        self.spark = SparkSessionManager(self.__class__.__name__).create_session()

    def implement_etl(self, config):
        etl_config = config.get("etl")

        # Read datasets based on ETL logic
        # Create temp view to replicate table like structure
        accounts = self.spark.read.parquet("../datasets/silver/accounts/")
        customers = self.spark.read.parquet("../datasets/silver/customers/")
        transactions = self.spark.read.parquet("../datasets/silver/transactions/")
        accounts.createOrReplaceTempView('accounts_table')
        customers.createOrReplaceTempView('customers_table')
        transactions.createOrReplaceTempView('transactions_table')

        accounts.show(20, 0)
        customers.show(20, 0)
        transactions.show(20, 0)

        # Initialize joined DataFrame
        joined_df = None

        # Perform joins dynamically
        for join_name, join_info in etl_config.items():
            if join_name.startswith("join"):
                left_table = join_info["left"]
                right_table = join_info["right"]
                left_key = join_info["left_key"]
                right_key = join_info["right_key"]
                join_type = join_info["join_type"]

                # Get DataFrames for left and right tables
                left_df = self.spark.sql(f"select * from {left_table}_table")
                right_df = self.spark.sql(f"select * from {right_table}_table")

                # Perform join operation
                if joined_df is None:
                    joined_df = left_df
                cond = left_df[left_key] == right_df[right_key]
                joined_df = joined_df.join(right_df, cond, join_type)

        # Perform aggregations dynamically
        aggregations = etl_config.get("aggregations", {})
        for agg_name, agg_sql in aggregations.items():
            result = joined_df.selectExpr(agg_sql).collect()[0]
            self.logger.info(f"{agg_name}: {result}")

        # joined_df.show(200, 0)

        final_df = joined_df.select("transactions_table.transaction_id",
                                    "transactions_table.account_number",
                                    "transactions_table.amount",
                                    "transactions_table.transaction_type",
                                    "customers_table.customer_id",
                                    "customers_table.name",
                                    "transactions_table.year",
                                    "transactions_table.month",
                                    "transactions_table.day")

        final_df = final_df.withColumn("date_id", concat_ws("-", "year", "month", "day")) \
            .drop('year', 'month', 'day')

        final_df.show(20, 0)

        # Write the joined DataFrame to the output path
        output_path = etl_config["output"]["path"]
        output_format = etl_config["output"]["format"]

        final_df.write.format(output_format) \
            .mode("overwrite") \
            .partitionBy('date_id') \
            .save(output_path)


if __name__ == '__main__':
    config_feed = "../config/bank.json"

    # read the raw data from bronze layer
    reader = DataReader(config_feed)
    datasets, config = reader.read_datasets()

    # write the data to silver layer in parquet format
    writer = DataWriter(datasets, config)
    writer.write_silver_output()

    '''
    TBD : identify & implement a change detection mechanism 
    1. File based (.in_process file)
    2. Comparing filenames bronze layer with already published/consumed filenames stored in a table. 
    For now : picking data from bronze
    '''

    # run etl pipeline
    BankPipeline().implement_etl(config)
