from pyspark.sql import SparkSession

# Create a Spark session

spark = SparkSession.builder.appName("RevenuePrediction").getOrCreate()

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

orders_df.createOrReplaceTempView("orders")
order_items_df.createOrReplaceTempView("order_items")
products_df.createOrReplaceTempView("products")

query = """
    SELECT
	        o._c1 as order_date,
        p._c2 as product_name,
        AVG(oi._c4) AS avg_revenue_per_day
    FROM
        orders o
    JOIN
        order_items oi ON o._c0 = oi._c1
    JOIN
        products p ON oi._c2 = p._c0
    GROUP BY
        o._c1, p._c2
"""

# Execute the query
result = spark.sql(query)

# Show the result
result.show()

column_names = ["order_date", "product_name", "avg_revenue_per_day"]

# Rename the columns
result = result.toDF(*column_names)

# Specify the full HDFS path including the desired file name
output_path = "hdfs://sandbox-hdp.hortonworks.com:8020//user/RevenuePrediction_data"

# Save the result to HDFS with the specified file name and column names
result.coalesce(1).write.option("header", "true").option("delimiter", ",").mode("overwrite").csv(output_path)



from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression



spark.stop()
