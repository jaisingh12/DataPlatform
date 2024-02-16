from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder \
    .appName("Data Transformation") \
    .getOrCreate()

# JSON data
json_data = {
  "transactions": [
    {
      "transaction_id": 1,
      "account_number": 1001,
      "amount": 100,
      "transaction_type": "Deposit"
    },
    {
      "transaction_id": 2,
      "account_number": 1001,
      "amount": 50,
      "transaction_type": "Withdrawal"
    },
    {
      "transaction_id": 3,
      "account_number": 1002,
      "amount": 200,
      "transaction_type": "Deposit"
    },
    {
      "transaction_id": 4,
      "account_number": 1003,
      "amount": 500,
      "transaction_type": "Withdrawal"
    },
    {
      "transaction_id": 5,
      "account_number": 1003,
      "amount": 300,
      "transaction_type": "Deposit"
    },
    {
      "transaction_id": 6,
      "account_number": 1001,
      "amount": 150,
      "transaction_type": "Withdrawal"
    }
  ]
}

# Create a DataFrame from the JSON data
df = spark.createDataFrame(json_data['transactions'])

# Show the DataFrame
df.show()

# Stop the SparkSession
spark.stop()
