from pyspark.sql import SparkSession

# Create a Spark session
spark = SparkSession.builder.appName("RetailStoreData").getOrCreate()

# Define the HDFS base path
hdfs_path_starting = "/user/"  
hdfs_path_finishing = "/part-m-00000.csv" 

# Read data from HDFS into DataFrames
customers_df = spark.read.format("csv").option("header", "true").load(hdfs_base_path + "customers" + hdfs_path_finishing )
departments_df = spark.read.format("csv").option("header", "true").load(hdfs_base_path + "departments" + hdfs_path_finishing)
categories_df = spark.read.format("csv").option("header", "true").load(hdfs_base_path + "categories" + hdfs_path_finishing)
orders_df = spark.read.format("csv").option("header", "true").load(hdfs_base_path + "orders" + hdfs_path_finishing)
order_items_df = spark.read.format("csv").option("header", "true").load(hdfs_base_path + "order_items" + hdfs_path_finishing)
products_df = spark.read.format("csv").option("header", "true").load(hdfs_base_path + "products" + hdfs_path_finishing)

# Now you have DataFrames for each of your data tables

# You can examine the schemas if needed
customers_df.printSchema()
departments_df.printSchema()
categories_df.printSchema()
orders_df.printSchema()
order_items_df.printSchema()
products_df.printSchema()

# You can perform further operations, such as joins, aggregations, etc., on these DataFrames as needed.
