import json
import os
import re

from utils.logger import Logger
from utils.spark_session import SparkSessionManager

from pyspark.sql.functions import *


class DataReader:
    def __init__(self, config_file):
        self.config_file = config_file
        self.logger = Logger(self.__class__.__name__)
        self.spark = None

    def _read_config(self):
        with open(self.config_file) as f:
            config = json.load(f)
            return config

    def _get_matching_files(self, dataset_path, regex):
        all_files = os.listdir(dataset_path)
        pattern = re.compile(regex)
        matching_files = [file for file in all_files if pattern.match(file)]
        return matching_files

    def read_datasets(self):
        # read config file for datasets
        config = self._read_config()

        # set spark session with spark conf
        spark_conf = config["spark_conf"]
        spark_manager = SparkSessionManager(self.__class__.__name__, spark_conf)
        self.spark = spark_manager.create_session()

        datasets = {}
        # read each dataset data
        for dataset_name, dataset_info in config['datasets'].items():
            data_format = dataset_info.get('format')
            dataset_path = dataset_info.get('input_path')
            regex = dataset_info.get("regex")

            matching_files = self._get_matching_files(dataset_path, regex)
            if not matching_files:
                self.logger.info(
                    f"No files found matching the regex {regex} for dataset {dataset_name} at loc {dataset_path}")
                continue

            dataset_info['matched_files'] = matching_files

            try:
                # Read CSV datasets
                if data_format == 'csv':
                    datasets[dataset_name] = self._read_csv(dataset_info)

                # Read JSON datasets
                elif data_format == 'json':
                    datasets[dataset_name] = self._read_json(dataset_info)

                # Show the dataset
                self.logger.info(f"Data loaded into spark dataframe for dataset - {dataset_name}")
                # datasets[dataset_name].show(truncate=False)

            except Exception as e:
                self.logger.error(f"Error reading dataset '{dataset_name}': {e}")

        return datasets, config

    def _read_csv(self, dataset_info):
        header = dataset_info.get('header', 'true')  # Default header to 'true' if not provided
        delimiter = dataset_info.get('delimiter', ',')  # Default delimiter to ',' if not provided
        matched_files = dataset_info.get("matched_files")
        dataset_path = dataset_info.get("input_path")

        for file in matched_files:
            df = self.spark.read.format("csv") \
                .option('header', header) \
                .option('delimiter', delimiter) \
                .load(f"{dataset_path}/{file}")
            df = self._add_metadata_columns(df, file)
        return df

    def _read_json(self, dataset_info):
        matched_files = dataset_info.get("matched_files")
        dataset_path = dataset_info.get("input_path")

        for file in matched_files:
            df = self.spark.read.format("json") \
                     .option("multiline", "true") \
                     .load(f"{dataset_path}/{file}")
            df = self._add_metadata_columns(df, file)

        return df

    def _add_metadata_columns(self, df, filename):
        # Extract date information from filename
        match = re.match(r'.*_(\d{4})(\d{2})(\d{2})\d{6}\..*', filename)
        year, month, day = match.group(1), match.group(2), match.group(3)

        # Add columns for year, month, day & filename
        df = df.withColumn("year", lit(year)) \
            .withColumn("month", lit(month)) \
            .withColumn("day", lit(day)) \
            .withColumn("filename", lit(filename))

        return df