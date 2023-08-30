from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("RetailStoreData").getOrCreate()

hdfs_path_starting = "hdfs://sandbox-hdp.hortonworks.com:8020//user/"  
hdfs_path_finishing = "/part-m-00000" 

customers_df = spark.read.format("csv").option("header", "false").load(hdfs_path_starting + "customers" + hdfs_path_finishing )
departments_df = spark.read.format("csv").option("header", "false").load(hdfs_path_starting + "departments" + hdfs_path_finishing)
categories_df = spark.read.format("csv").option("header", "false").load(hdfs_path_starting + "categories" + hdfs_path_finishing)
orders_df = spark.read.format("csv").option("header", "false").load(hdfs_path_starting + "orders" + hdfs_path_finishing)
order_items_df = spark.read.format("csv").option("header", "false").load(hdfs_path_starting + "order_items" + hdfs_path_finishing)
products_df = spark.read.format("csv").option("header", "false").load(hdfs_path_starting + "products" + hdfs_path_finishing)

customers_df.printSchema()
departments_df.printSchema()
categories_df.printSchema()
orders_df.printSchema()
order_items_df.printSchema()
products_df.printSchema()


"""
By using the SparkSQL with pySpark, write a script to:

- Get How many Orders were placed
- Get Average Revenue Per Order
- Get Average Revenue Per Day Per Product

"""
orders_df.createOrReplaceTempView("orders")

query1 = """
    SELECT COUNT(orders._c0) AS Number_of_Orders_Placed FROM orders
"""
result = spark.sql(query1)

result.show()

#--------------------------------------------------

order_items_df.createOrReplaceTempView("order_items")

# Define the Spark SQL query to calculate the average revenue per order
query2 = """
    SELECT orders._c0 AS order_id, AVG(order_items._c4) AS average_revenue_per_order
    FROM order_items INNER JOIN orders ON order_items._c1 = orders._c0
    GROUP BY orders._c0
"""
result = spark.sql(query2)

result.show()

#-------------------------------------------

products_df.createOrReplaceTempView("products")

query3 = """
    SELECT
    o._c1 AS order_date,
    p._c0 AS Product_id,
    p._c2 AS Product_name,
    AVG(oi._c4) AS average_revenue_per_product
    FROM orders o
    JOIN order_items oi ON o._c0 = oi._c1
    JOIN products p ON oi._c2 = p._c0
    GROUP BY o._c1, p._c0, p._c2
    ORDER BY o._c1, p._c0
"""
result = spark.sql(query3)

result.show()

spark.stop()