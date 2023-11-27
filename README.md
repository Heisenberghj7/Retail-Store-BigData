# 📉 🧑‍💻 Retail Store BigData 📊📦
## Project Architecture 📐 🖊️
<img src="retail_project.png">

## Part I: Data Migration & Data Analysis

**Importing a Table from MySQL to HDFS:**

1. Create the database and the tables in MySQL.
2. Use Sqoop to import the tables in the retail store database and save it in HDFS under "/user".
3. Import the tables to a Parquet data format rather than the default file form (text file).

**Data Analysis:**
First of all we're going to import data from HDFS to Hive, HiveQL is Hive’s query language, a dialect of SQL for big data. By using HiveQL we're going to determine:

- Get How many Orders were placed
- Get Average Revenue Per Order
- Get Average Revenue Per Day Per Product

## Part ll : Spark SQL and PySpark
- 📫 Feel free to contact me if anything is wrong or if anything needs to be changed 😎!  **medhajjari9@gmail.com**

<a href="https://colab.research.google.com/github/heisenberghj7/Retail-Store-BigData/" target="_blank"><img src="https://colab.research.google.com/assets/colab-badge.svg" alt="Open In Colab"/></a>