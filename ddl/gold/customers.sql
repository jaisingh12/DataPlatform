create table customers (
 customer_id string
,name string
,age string
,gender string
,city string)
PARTITIONED BY (date_id date)
STORED AS PARQUET
LOCATION './datasets/gold/customers'
