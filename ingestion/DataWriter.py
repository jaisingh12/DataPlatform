from utils.logger import Logger

class DataWriter:
    def __init__(self, datasets, config):
        self.datasets = datasets
        self.config = config
        self.logger = Logger(self.__class__.__name__)

    def write_output(self):
        try:
            for dataset_name, df in self.datasets.items():
                self.logger.info(f"Loading Data to Silver Layer for dataset - {dataset_name}")

                df.write.format("parquet") \
                        .mode("overwrite") \
                        .partitionBy('year', 'month', 'day') \
                        .save(f"../datasets/silver/{dataset_name}")

                self.logger.info(f"Data loaded to Silver Layer for dataset - {dataset_name}")
        except Exception as e:
            self.logger.error(e)
