# Databricks notebook source
from pyspark.sql.functions import col, when,count,to_date,regexp_replace,to_timestamp,split
from pyspark.sql.types import StructType,StructField,StringType,IntegerType,DateType,DoubleType,TimestampType
import pyspark.sql.functions as f

# COMMAND ----------

# Mount Bronze Container to Databricks 
dbutils.fs.mount(
    source = "wasbs://bayerstorage@bayershackadls.blob.core.windows.net/Squad_3/",
    mount_point = "/mnt/",
    extra_configs = {
        "fs.azure.account.key.bayershackadls.blob.core.windows.net": "EjNBIgEXzka8sVjRAJ4QgCEJ411ETM6wkN/IcQYJOLt+qSmzs3YzrKFjrDt5Pm3Sb49tc9ZpFRiA+AStNrmeVg=="
    }
)

# COMMAND ----------

#Define Schema for the dataframe

schema = StructType([
    StructField("customer_id", IntegerType(), nullable=True),
    StructField("order_id", IntegerType(), nullable=True),
    StructField("order_date", StringType(), nullable=True),  # This will hold the timestamp
    StructField("order_channel", StringType(), nullable=True),
    StructField("store_code", StringType(), nullable=True),
    StructField("total_sales", DoubleType(), nullable=True),
    StructField("state", StringType(), nullable=True),  # Allowing NULL for state
    StructField("country", StringType(), nullable=True)
])

# COMMAND ----------

# Read the Orders Data from Bronze Layer
df = spark.read.option("header",True).schema(schema).csv("dbfs:/mnt/landing_zone/order.csv")
#display(df)

# COMMAND ----------

schema_df1 = StructType([
    StructField("order_line_id", IntegerType(), nullable=True),
    StructField("order_id", IntegerType(), nullable=True),
    StructField("product_id", StringType(), nullable=True),
    StructField("quantity", IntegerType(), nullable=True),
    StructField("price", DoubleType(), nullable=True),
    StructField("order_curr", StringType(), nullable=True)
])

# COMMAND ----------

# Read the Orders Line Data from Bronze Layer
df1 = spark.read.option("header",True).schema(schema_df1).csv("dbfs:/mnt/landing_zone/order_line.csv")

df1.write.mode("overwrite").parquet("dbfs:/mnt/Squad_3/silver/order_Lines_cleaned.parquet")

display(df1)

# COMMAND ----------

# Data Validation Checks

# Remove special characters from store_code column
df_removesplchar = df.withColumn("store_code", regexp_replace(col("store_code"), "[^a-zA-Z0-9]", ""))

# Filter out rows where state is null
df_filternull = df_removesplchar.filter(df_removesplchar["state"].isNotNull())

# Filter negative values in total_sales column
df_filterednegvalue = df_filternull.filter(df["total_sales"] >= 0)

df_filterednegvalue.write.mode("overwrite").parquet("dbfs:/mnt/Squad_3/silver/order_cleaned.parquet")

#display(df_filterednegvalue)

# COMMAND ----------

from pyspark.sql.functions import round

df_final = df_filterednegvalue.join(df1, df_filterednegvalue.order_id == df1.order_id, 'left_outer').select("customer_id", df_filterednegvalue["order_id"], "order_date", "order_channel", "store_code", "product_id", "Quantity", "price", "order_Curr")
df_final = df_final.withColumn('Total_Sales', round(df_final["Quantity"] * df_final["price"], 2))
display(df_final)

# COMMAND ----------

df_final.write.mode("overwrite").parquet("dbfs:/mnt/Squad_3/silver/order_joined.parquet")
