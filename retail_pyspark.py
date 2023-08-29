from pyspark.sql import SparkSession

# Create a Spark session

spark = SparkSession.builder.appName("RetailStoreData").getOrCreate()

# Define the HDFS base path
hdfs_path_starting = "hdfs://sandbox-hdp.hortonworks.com:8020//user/"  
hdfs_path_finishing = "/part-m-00000" 

# Read data from HDFS into DataFrames
customers_df = spark.read.format("csv").option("header", "false").load(hdfs_path_starting + "customers" + hdfs_path_finishing )
departments_df = spark.read.format("csv").option("header", "false").load(hdfs_path_starting + "departments" + hdfs_path_finishing)
categories_df = spark.read.format("csv").option("header", "false").load(hdfs_path_starting + "categories" + hdfs_path_finishing)
orders_df = spark.read.format("csv").option("header", "false").load(hdfs_path_starting + "orders" + hdfs_path_finishing)
order_items_df = spark.read.format("csv").option("header", "false").load(hdfs_path_starting + "order_items" + hdfs_path_finishing)
products_df = spark.read.format("csv").option("header", "false").load(hdfs_path_starting + "products" + hdfs_path_finishing)

# Now you have DataFrames for each of your data tables

# You can examine the schemas if needed
customers_df.printSchema()
departments_df.printSchema()
categories_df.printSchema()
orders_df.printSchema()
order_items_df.printSchema()
products_df.printSchema()

# You can perform further operations, such as joins, aggregations, etc., on these DataFrames as needed.