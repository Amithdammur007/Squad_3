# Databricks notebook source
# Databricks notebook source
# Mount an Azure Blob Storage blob container to Databricks

if any(mount.mountPoint == '/mnt/bronze' for mount in dbutils.fs.mounts()):
    dbutils.fs.unmount('/mnt/bronze')

dbutils.fs.mount(
    source = "wasbs://bayerstorage@bayershackadls.blob.core.windows.net/Squad_3/bronze",
    mount_point = "/mnt/bronze",
    extra_configs = {
        "fs.azure.account.key.bayershackadls.blob.core.windows.net": "EjNBIgEXzka8sVjRAJ4QgCEJ411ETM6wkN/IcQYJOLt+qSmzs3YzrKFjrDt5Pm3Sb49tc9ZpFRiA+AStNrmeVg=="
    }
)

# COMMAND ----------

# Mount an Azure Blob Storage silver container to Databricks

if any(mount.mountPoint == '/mnt/silver' for mount in dbutils.fs.mounts()):
    dbutils.fs.unmount('/mnt/silver')

dbutils.fs.mount(
    source = "wasbs://bayerstorage@bayershackadls.blob.core.windows.net/Squad_3/silver",
    mount_point = "/mnt/silver",
    extra_configs = {
        "fs.azure.account.key.bayershackadls.blob.core.windows.net": "EjNBIgEXzka8sVjRAJ4QgCEJ411ETM6wkN/IcQYJOLt+qSmzs3YzrKFjrDt5Pm3Sb49tc9ZpFRiA+AStNrmeVg=="
    }
)

# COMMAND ----------

# Mount an Azure Blob Storage gold container to Databricks

if any(mount.mountPoint == '/mnt/gold' for mount in dbutils.fs.mounts()):
    dbutils.fs.unmount('/mnt/gold')

dbutils.fs.mount(
    source = "wasbs://bayerstorage@bayershackadls.blob.core.windows.net/Squad_3/gold",
    mount_point = "/mnt/gold",
    extra_configs = {
        "fs.azure.account.key.bayershackadls.blob.core.windows.net": "EjNBIgEXzka8sVjRAJ4QgCEJ411ETM6wkN/IcQYJOLt+qSmzs3YzrKFjrDt5Pm3Sb49tc9ZpFRiA+AStNrmeVg=="
    }
)

# COMMAND ----------

# Mount an Azure Blob Storage gold container to Databricks
if any(mount.mountPoint == '/mnt/bad_data' for mount in dbutils.fs.mounts()):
    dbutils.fs.unmount('/mnt/bad_data')

dbutils.fs.mount(
    source = "wasbs://bayerstorage@bayershackadls.blob.core.windows.net/Squad_3/bad_data",
    mount_point = "/mnt/bad_data",
    extra_configs = {
        "fs.azure.account.key.bayershackadls.blob.core.windows.net": "EjNBIgEXzka8sVjRAJ4QgCEJ411ETM6wkN/IcQYJOLt+qSmzs3YzrKFjrDt5Pm3Sb49tc9ZpFRiA+AStNrmeVg=="
    }
)

# COMMAND ----------

bronze_path = "/mnt/bronze"
silver_path = "/mnt/silver"
gold_path = "/mnt/gold"
bad_rc_path = '/mnt/bad_data' 

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, to_date
from pyspark.sql.types import *

# Define schema
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
df_customer = spark.read.option("header", "true").schema(cus_schema).csv(bronze_path+'/customer.csv')


# Validation: Check for null values
df_customer_br = df_customer.withColumn("is_valid", when(col("customer_id").isNull(), False).otherwise(True)).filter("is_valid == False")

df_customer_br.write.mode("overwrite").csv(bad_rc_path+"/bad_records.csv")

df_customer = df_customer.where("customer_id is not null")

display(df_customer)

# COMMAND ----------

# Validation: Check for invalid email format
df_bad_email = df_customer.withColumn("is_valid", when(~col("email").rlike("^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,6}$"), False).otherwise(True)).filter("is_valid == False")

df_bad_email.write.mode("append").csv(bad_rc_path+"/bad_records.csv")

df_customer = df_customer.where("email is not null")

display(df_customer)

# COMMAND ----------

# Validation: Check for valid phone number format (assuming 10 digits with optional hyphens)
df_bad_ph = df_customer.withColumn("is_valid", when(~col("phone").rlike("^[0-9]{3}-[0-9]{3}-[0-9]{4}$"), False).otherwise(True))

df_bad_ph.write.mode("append").csv(bad_rc_path+"/bad_records.csv")

df_customer = df_customer.where("phone is not null")

display(df_customer)


# COMMAND ----------


from pyspark.sql.functions import * 

df_customer = df_customer.withColumn("created_date", to_timestamp(col("created_date"), "MM/dd/yyyy HH:m"))
df_customer = df_customer.withColumn("last_updated_date", to_timestamp(col("last_updated_date"), "MM/dd/yyyy HH:mm"))

display(df_customer)

# COMMAND ----------

# Date data converted to date type

df_customer = df_customer.filter('active == True').drop("is_Valid")

display(df_customer)

# COMMAND ----------

# Validation: Ensure there are no duplicate customer_ids
unique_count = df_customer.agg(countDistinct("customer_id")).collect()[0][0]
total_count = df_customer.count()

print(f"Duplicate records : {total_count - unique_count}" )

# deleting duplicates

df_customer = df_customer.dropDuplicates()
print(f"Unique reords : {df_customer.count()}")




# COMMAND ----------

# write data to silver layer

df_customer.write.mode("overwrite").csv("/mnt/silver/customer_cleansed.csv")



# COMMAND ----------

cus_order = StructType([
    StructField("customer_id", IntegerType(), True),
    StructField("order_id", IntegerType(), True),
    StructField("order_channel", StringType(), True),
    StructField("store_code", StringType(), True),
    StructField("total_purchase_value", DoubleType(), True),
    StructField("state", StringType(), True),
    StructField("order_country", StringType(), True),
])

df_order = spark.read.option("header","true").option("inferSchema","true").csv(bronze_path+'/order.csv')


df_order = df_order.dropDuplicates()

df_order = df_order.withColumnRenamed("customer_id","order_cus_id")

display(df_order)


# COMMAND ----------

#join
df_joined = df_customer.join(df_order, df_customer.customer_id == df_order.order_cus_id, "inner")

df_cus_fact = df_joined.select("customer_id","First_name","last_name","gender","address","country","order_id","total_purchase_value","store_code")

#write to gold layer

df_cus_fact.write.mode("overwrite").csv(gold_path+"/customer_fact")



# COMMAND ----------


# Total purchse made by each customer , displayed in descending order

df_agg = df_cus_fact.groupBy("First_name","last_name").agg(sum("total_purchase_value").alias("total_amount_per_customer")).orderBy(desc("total_amount_per_customer"))

display(df_agg)
#Store wise sales

df_store_wise = df_cus_fact.select("store_code" , "total_purchase_value","country")

df_store_sales = df_store_wise.groupBy("store_code","country").agg(sum("total_purchase_value").alias("store_wise_sales")).orderBy("store_code")

display(df_store_sales)

