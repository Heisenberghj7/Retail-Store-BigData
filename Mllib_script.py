# Create a Spark session

# spark = SparkSession.builder.appName("RevenuePrediction").getOrCreate()

# # Define the HDFS base path
# hdfs_path_starting = "hdfs://sandbox-hdp.hortonworks.com:8020//user/"  
# hdfs_path_finishing = "/part-m-00000" 

# # Read data from HDFS into DataFrames
# customers_df = spark.read.format("csv").option("header", "false").load(hdfs_path_starting + "customers" + hdfs_path_finishing )
# departments_df = spark.read.format("csv").option("header", "false").load(hdfs_path_starting + "departments" + hdfs_path_finishing)
# categories_df = spark.read.format("csv").option("header", "false").load(hdfs_path_starting + "categories" + hdfs_path_finishing)
# orders_df = spark.read.format("csv").option("header", "false").load(hdfs_path_starting + "orders" + hdfs_path_finishing)
# order_items_df = spark.read.format("csv").option("header", "false").load(hdfs_path_starting + "order_items" + hdfs_path_finishing)
# products_df = spark.read.format("csv").option("header", "false").load(hdfs_path_starting + "products" + hdfs_path_finishing)

# orders_df.createOrReplaceTempView("orders")
# order_items_df.createOrReplaceTempView("order_items")
# products_df.createOrReplaceTempView("products")

# query = """
#     SELECT
# 	        o._c1 as order_date,
#         p._c2 as product_name,
#         AVG(oi._c4) AS avg_revenue_per_day
#     FROM
#         orders o
#     JOIN
#         order_items oi ON o._c0 = oi._c1
#     JOIN
#         products p ON oi._c2 = p._c0
#     GROUP BY
#         o._c1, p._c2
# """

# # Execute the query
# result = spark.sql(query)

# # Show the result
# result.show()

# column_names = ["order_date", "product_name", "avg_revenue_per_day"]

# # Rename the columns
# result = result.toDF(*column_names)

# # Specify the full HDFS path including the desired file name
# output_path = "hdfs://sandbox-hdp.hortonworks.com:8020//user/RevenuePrediction_data"

# # Save the result to HDFS with the specified file name and column names
# result.coalesce(1).write.option("header", "true").option("delimiter", ",").mode("overwrite").csv(output_path)
from pyspark.sql import SparkSession
from pyspark.ml.regression import LinearRegression
from pyspark.ml.feature import StringIndexer, OneHotEncoder
from pyspark.ml.feature import VectorAssembler
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql.functions import dayofweek

spark = SparkSession.builder.appName("RevenuePrediction").getOrCreate()
training_data_path = "hdfs://sandbox-hdp.hortonworks.com:8020//user/RevenuePrediction_data/output.csv"
data = spark.read.csv(training_data_path , header=True, inferSchema=True)


# Convert order_date to day of the week (1-7, where 1=Sunday, 7=Saturday)
data = data.withColumn("day_of_week", dayofweek(data["order_date"]))

# Assuming 'product_name' is a categorical variable
product_indexer = StringIndexer(inputCol="product_name", outputCol="product_index")
product_encoder = OneHotEncoder(inputCol="product_index", outputCol="product_vector")

# Prepare features vector
feature_cols = ["product_vector", "day_of_week"]
feature_assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")

# Split data into training and testing sets
train_data, test_data = data.randomSplit([0.7, 0.3], seed=42)

lr = LinearRegression(featuresCol="features", labelCol="avg_revenue_per_day")
pipeline = Pipeline(stages=[product_indexer, product_encoder, feature_assembler, lr])
model = pipeline.fit(train_data)
predictions = model.transform(test_data)

evaluator = RegressionEvaluator(labelCol="avg_revenue_per_day", predictionCol="prediction", metricName="mse")
mse = evaluator.evaluate(predictions)
print("Mean Squared Error: {}".format(mse))

spark.stop()
