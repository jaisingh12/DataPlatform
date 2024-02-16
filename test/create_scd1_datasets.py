import re

from pyspark.sql.functions import concat_ws, when, concat, lit, length

from utils.spark_session import SparkSessionManager

spark = SparkSessionManager("test").create_session()


def _read_csv(path):
    df = spark.read.format("csv") \
        .option('header', "true") \
        .option('delimiter', "|") \
        .load(path)
    return df


def _add_metadata_columns(df, file):
    # Extract date information from filename
    match = re.match(r'.*_(\d{4})(\d{2})(\d{2})\d{6}\..*', file)
    year, month, day = match.group(1), match.group(2), match.group(3)

    # Add columns for year, month, day & filename
    df = df.withColumn("year", lit(year)) \
        .withColumn("month", when(length(lit(month)) < 2, concat(lit("0"), lit(month))).otherwise(lit(month))) \
        .withColumn("day", when(length(lit(day)) < 2, concat(lit("0"), lit(day))).otherwise(lit(day))) \
        .withColumn("filename", lit(file))

    return df


#################### Create Target dataset @Gold layer #############
path = "../datasets/bronze/accounts/accounts_20240101060606.csv"
file = path.split("/")[-1]
df = _read_csv(path)
df = _add_metadata_columns(df, file)

df = df.withColumn("date_id", concat_ws("-", "year", "month", "day")) \
    .drop("year", "month", "day", "filename")

df.show()

df.write.partitionBy('date_id').mode('overwrite').parquet("../datasets/gold/accounts/")

#################### Create Source dataset @silver layer #############
path = "../datasets/bronze/accounts/accounts_20240101080808.csv"
file = path.split("/")[-1]
df = _read_csv(path)
df = _add_metadata_columns(df, file)

df.show()

df.write.partitionBy('year', 'month', 'day').mode('overwrite').parquet("../datasets/silver/accounts/")
