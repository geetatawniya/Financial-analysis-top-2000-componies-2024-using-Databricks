# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## Overview
# MAGIC
# MAGIC This notebook will show you how to create and query a table or DataFrame that you uploaded to DBFS. [DBFS](https://docs.databricks.com/user-guide/dbfs-databricks-file-system.html) is a Databricks File System that allows you to store data for querying inside of Databricks. This notebook assumes that you have a file already inside of DBFS that you would like to read from.
# MAGIC
# MAGIC This notebook is written in **Python** so the default cell type is Python. However, you can use different languages by using the `%LANGUAGE` syntax. Python, Scala, SQL, and R are all supported.

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, monotonically_increasing_id

# COMMAND ----------

import pyspark

# COMMAND ----------

# MAGIC %fs ls dbfs:/FileStore/tables/
# MAGIC

# COMMAND ----------

file_path="dbfs:/FileStore/tables/Top_2000_Companies_Financial_Data_2024-1.csv"
file_type = "csv"

# CSV options
infer_schema = "false"
first_row_is_header = "false"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
df1 = spark.read.format("csv") \
  .option("inferSchema", infer_schema) \
  .option("header",True) \
  .option("sep", delimiter)\
  .load(file_path)
 

display(df1)

# COMMAND ----------

# File location and type
file_location = "/FileStore/tables/Telco_customer_churn_location.csv"
file_type = "csv"

# CSV options
infer_schema = "false"
first_row_is_header = "false"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
df2 = spark.read.format("csv") \
  .option("inferSchema", infer_schema) \
  .option("header",True) \
  .option("sep", delimiter) \
  .load(file_location)

display(df2)

# COMMAND ----------

# Create a view or table

temp_table_name = "Telco_customer_churn_location"

df2.createOrReplaceTempView(temp_table_name)

sqldF = spark.sql("SELECT * FROM Telco_customer_churn_location  WHERE City='San Diego' and `Zip Code`  < 92300 ") 
sqldF.display()


# COMMAND ----------

#CREATE TEMPORARY TABLE
Temp_table_name2 = "country_bank_info"

df1.createOrReplaceTempView(Temp_table_name2)

SQLDF1=spark.sql(
    """SELECT Country, COUNT(*) as count
    FROM country_bank_info
    GROUP BY Country""") 
SQLDF1.display()


# COMMAND ----------

df2.filter(df2.City == "San Diego").show()

# COMMAND ----------

sqldF = spark.sql("SELECT * FROM Telco_customer_churn_location WHERE LOWER(City) = 'San Diego'")
sqldF.display()


# COMMAND ----------

df.select("City").distinct().show(100, truncate=False)

# COMMAND ----------

#df.filter(df.City == "San Diego").show()
from pyspark.sql.functions import trim
df2 = df2.withColumn("City", trim(df2.City))
df2.display()

# COMMAND ----------

from pyspark.sql.functions import trim
df = df.withColumn("City", trim(df.City))
df.createOrReplaceTempView(temp_table_name)
sqldF = spark.sql("SELECT * FROM Telco_customer_churn_location WHERE City = 'San Diego'")
sqldF.display()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.show()
print(f"Row count: {df.count()}")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC /* Query the created temp table in a SQL cell */
# MAGIC
# MAGIC select * from Telco_customer_churn_location where City='San Diego'

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# With this registered as a temp view, it will only be available to this particular notebook. If you'd like other users to be able to query this table, you can also create a table from the DataFrame.
# Once saved, this table will persist across cluster restarts as well as allow various users across different notebooks to query this data.
# To do so, choose your table name and uncomment the bottom line.

permanent_table_name = "Telco_customer_churn_demographics_xlsx"

# df.write.format("parquet").saveAsTable(permanent_table_name)

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType 

schema = StructType([     StructField("Customer_ID", StringType(), True),
                          StructField("Count", IntegerType(), True),
                          StructField("Country", StringType(), True),
                          StructField("State", StringType(), True),
                          StructField("City", StringType(), True),
                          StructField("Zip_Code", IntegerType(), True),
                          StructField("Lat_Long", IntegerType(), True),
                          StructField("Latitude", IntegerType(), True),
                          StructField("Longitude", IntegerType(), True)
                           ]) 
df = spark.read.schema(schema).csv("/FileStore/tables/Telco_customer_churn_location.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC 4. Transformations and Actions:
# MAGIC
# MAGIC Description: Transformations (like filter, map, select) are lazy operations that define a new DataFrame, while actions (like collect, show, count) trigger execution.
# MAGIC

# COMMAND ----------

df_filter=df.Count(df.'Zip Code' > 92129)
df_filter.show()

# COMMAND ----------

from pyspark.sql import SparkSession 
spark = SparkSession.builder.appName("example").getOrCreate()

# COMMAND ----------

rdd = spark.sparkContext.parallelize([1, 2, 3, 4, 5]) rdd.map(lambda x: x * x).collect()

# COMMAND ----------

# MAGIC %md
# MAGIC SQL and DataFrame API:
# MAGIC
# MAGIC Description: Using SQL queries to interact with DataFrames.
# MAGIC Example:

# COMMAND ----------

# MAGIC %sql
# MAGIC  --Create a view or table
# MAGIC
# MAGIC temp_table_name = "Telco_customer_churn_location"
# MAGIC
# MAGIC df.createOrReplaceTempView(temp_table_name)
# MAGIC sqldF = spark.sql("SELECT * FROM temp_table_name WHERE Customer_ID='1' ") 
# MAGIC sqldF.display()

# COMMAND ----------

df.createOrReplaceTempView("TelcoCustomer") 
sqldF = spark.sql("SELECT * FROM TelcoCustomer WHERE Customer_ID='1' ") 
sqldF.display()

# COMMAND ----------

df1 = spark.read.csv("TelcoCustomer1", header=True) 
df2 = spark.read.csv("TelcoCustomer.csv", header=True) 
#joined_df = df1.join(df2, df1.id == df2.id, "inner") 
#joined_df.show()