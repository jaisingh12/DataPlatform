from utils.logger import Logger


def write_output(df, path, file_format, write_mode, part_cols):
    df.write.format(file_format) \
        .mode(write_mode) \
        .partitionBy(part_cols) \
        .save(path)


class DataWriter:
    def __init__(self, datasets, config):
        self.datasets = datasets
        self.config = config
        self.logger = Logger(self.__class__.__name__)

    def write_silver_output(self):
        try:
            for dataset_name, df in self.datasets.items():
                self.logger.info(f"Loading Data to Silver layer for dataset - {dataset_name}")

                path = self.config['datasets'][dataset_name]['output_path']
                file_format = "parquet"
                write_mode = "overwrite"
                part_cols = ['year', 'month', 'day']

                write_output(df, path, file_format, write_mode, part_cols)

                self.logger.info(f"Data loaded to Silver Layer for dataset - {dataset_name}")
        except Exception as e:
            raise Exception(e)
