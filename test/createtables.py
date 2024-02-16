from utils.spark_session import SparkSessionManager
spark = SparkSessionManager("test").create_session()

spark.sql("select * from bank_str1").show()

spark.sql("select * from bank_str2").show()


table_ddl = """
CREATE TABLE bank_str2 (
 transaction_id long 
,account_number long
,amount long
,transaction_type string
,customer_id string
,name string)
USING DELTA 
PARTITIONED BY (date_id string)
LOCATION '../datasets/gold/bank'
"""

spark.sql(table_ddl).show()
spark.sql("select * from bank_str2").show()

spark.sql("describe extended bank_str2").show(1000,False)
spark.catalog.refreshTable("bank_str1")

spark.sql("select * from bank_str1").printSchema()


spark.sql("show tables").show(200,0)
spark.sql("select * from bank").printSchema()


#spark.sql("show tables in gold").show(200,0)

spark.sql("CREATE DATABASE IF NOT EXISTS gold LOCATION 'datasets/gold/'")

#df = spark.read.format("delta").load('../datasets/gold/bank/')
#df.show(20,0)

#df.printSchema()

