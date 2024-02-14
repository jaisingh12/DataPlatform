<h6>
This is an initial draft version of a dynamic data platform leveraging PySpark.
</h6>

This platform allows for seamless ingestion and ETL processes, driven by configuration-based files for generic data pipelines.

We have followed medallion/"multi-hop" architecture, to incrementally and progressively improve the structure and quality of data as it flows through each layer of the architecture. 
Bronze ⇒ Silver ⇒ Gold layer tables

In our bronze layer, we house raw feeds, from CSV to JSON, allowing ingestion using generic code lines. 
The silver layer elevates data with each day's feed, setting the stage for external Hive tables for streamlined access.
In our gold layer, where PySpark orchestrates transformative ETL processes such as joins, filters, and aggregations and 
finally produces the report which will be partitioned on date_id for easy querying.

<h6> Future Addition: </h6>
1. Different new data sources : Clouds buckets, streaming data
2. QA framework for data validation at the silver layer.
3. SCD1 and SCD2 implemetation.
4. Incremental Surrgoate key generation.
5. Containerized airflow and spark env for easy dev & deployments.
6. Create Airflow dags for the whole orchestration.
7. Dashboard to visualise datasets on a day to day basis.
8. setup hive/hive metastore and create external tables and use those tables.
   
