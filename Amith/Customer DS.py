# Databricks notebook source
from pyspark.sql.functions import col, when,count,to_date,regexp_replace,to_timestamp,split
from pyspark.sql.types import StructType,StructField,StringType,IntegerType,DateType,DoubleType,TimestampType
import pyspark.sql.functions as f

# COMMAND ----------

#Define Schema for Customer Data frame
cus_schema = StructType([
    StructField("customer_id", IntegerType(), True),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("Address", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("country", StringType(), True),
    StructField("zipcode", IntegerType(), True),
    StructField("phone", StringType(), True),
    StructField("created_date", StringType(), True),
    StructField("last_updated_date", StringType(), True),
    StructField("active", StringType(), True)
])

# Load CSV file into DataFrame
df_customer = spark.read.option("header", "true").schema(cus_schema).csv('/mnt/landing_zone/customer.csv')

# Validation: Check for null values
df_customer = df_customer.withColumn("is_valid", when(col("customer_id").isNull(), False).otherwise(True))

# Validation: Check for invalid email format
df_customer = df_customer.withColumn("is_valid", when(~col("email").rlike("^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,6}$"), False).otherwise(col("is_valid")))

# Validation: Check for valid phone number format (assuming 10 digits)
df_customer = df_customer.withColumn("is_valid", when(~col("phone").rlike("^[0-9]{10}$"), False).otherwise(col("is_valid")))

# Validation: Ensure dates are in valid format and convert to date type
df_customer = df_customer.withColumn("created_date", to_date(col("created_date"), "yyyy-MM-dd"))
df_customer = df_customer.withColumn("last_updated_date", to_date(col("last_updated_date"), "yyyy-MM-dd"))
df_customer = df_customer.withColumn("is_valid", when(col("created_date").isNull() | col("last_updated_date").isNull(), False).otherwise(col("is_valid")))

# Validation: Ensure 'active' is either 'true' or 'false'
df_customer = df_customer.withColumn("is_valid", when(~col("active").isin("true", "false"), False).otherwise(col("is_valid")))

# Separate valid and invalid records
valid_records = df_customer.filter(col("is_valid") == True).drop("is_valid")
invalid_records = df_customer.filter(col("is_valid") == False).drop("is_valid")

# Save valid and invalid records to different locations
valid_records.write.mode("overwrite").parquet("dbfs:/mnt/Squad_3/silver/customer_valid_records.parquet")

invalid_records.write.mode("overwrite").parquet("dbfs:/mnt/Squad_3/silver/customer_invalid_records.parquet")




