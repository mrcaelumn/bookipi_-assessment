from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, explode, lit

bucket_name = "test_bucket"
# Initialize Spark session
spark = SparkSession.builder \
    .appName("MongoDBToGCS") \
    .config("spark.mongodb.input.uri", "mongodb://127.0.0.1:27017/test_bookipi") \
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.0") \
    .getOrCreate()

# 1. Extract Data from MongoDB Collections
company_df = spark.read.format("mongo").option("collection", "company").load()
invoices_df = spark.read.format("mongo").option("collection", "invoices").load()
subscriptions_df = spark.read.format("mongo").option("collection", "subscription").load()
users_df = spark.read.format("mongo").option("collection", "user").load()

# 2. Data Cleanup and Transformation

# Rename shorthand fields
company_df = company_df.withColumnRenamed("e", "company_email") \
                       .withColumnRenamed("c", "company_name") \
                       .withColumnRenamed("co", "country") \
                       .withColumnRenamed("st", "state") \
                       .withColumnRenamed("_id", "company_id") \
                       .withColumnRenamed("cr", "currency")

users_df = users_df.withColumnRenamed("e", "email") \
                   .withColumnRenamed("f", "first_name") \
                   .withColumnRenamed("l", "last_name")

# Flatten nested invoice items and payments
invoices_flat_df = invoices_df.withColumn("item", explode(col("it"))) \
                              .withColumn("payment", explode(col("pm")))

# Extract key fields from nested structures
invoices_clean_df = invoices_flat_df.select(
    col("_id").alias("invoice_id"),
    col("ci").alias("company_id"),
    col("item.name").alias("item_name"),
    col("item.quantity").alias("quantity"),
    col("item.price").alias("price"),
    col("payment.date").alias("payment_date"),
    col("payment.amount").alias("payment_amount")
)



# Ensure subscription status defaults to 'terminated' if null
subscriptions_df = subscriptions_df.withColumn(
    "status", when(col("status").isNull(), "terminated").otherwise(col("status"))
)
company_df.show()
users_df.show()
invoices_clean_df.show()
subscriptions_df.show()
# Join user and company data for additional context
# user_company_df = users_df.join(company_df, col("dci") == col("_id"), "left")
user_company_df = users_df.join(company_df, users_df["dci"] == company_df["company_id"], "left")
user_company_df.show()
# 3. Write Cleaned Data to GCS in Parquet Format
# invoices_clean_df.write.mode("overwrite").parquet(f"gs://{bucket_name}/invoices/")
# subscriptions_df.write.mode("overwrite").parquet(f"gs://{bucket_name}/subscriptions/")
# user_company_df.write.mode("overwrite").parquet(f"gs://{bucket_name}/users/")


invoices_clean_df.write.mode("overwrite").parquet(f"output/invoices/")
subscriptions_df.write.mode("overwrite").parquet(f"output/subscriptions/")
user_company_df.write.mode("overwrite").parquet(f"output/users/")

spark.stop()
