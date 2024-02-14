from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip


class SparkSessionManager:
    def __init__(self, app_name, spark_conf=None):
        self.app_name = app_name
        self.spark_conf = spark_conf if spark_conf else {}

    def create_session(self):
        # Create a SparkSession
        spark_builder = SparkSession.builder.appName(self.app_name) \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

        # Apply Spark configuration
        for key, value in self.spark_conf.items():
            spark_builder.config(key, value)

        spark = configure_spark_with_delta_pip(spark_builder).getOrCreate()
        return spark
