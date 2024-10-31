from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split

# Initialize Spark session
spark = SparkSession.builder.appName("BillingPeriodParsing")\
    .config("spark.mongodb.input.uri", "mongodb://127.0.0.1:27017/test_bookipi") \
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.0") \
    .getOrCreate()

# Load Subscription Payments collection from MongoDB
subscription_payments_df = spark.read.format("mongo") \
    .option("collection", "subscription_payment").load() 

subscription_payments_df.show()
# Parse 'billingPeriod' field into 'start_date' and 'end_date'
parsed_df = subscription_payments_df.withColumn(
    "start_date", split(col("billingPeriod"), "-").getItem(0)
).withColumn(
    "end_date", split(col("billingPeriod"), "-").getItem(1)
)
parsed_df.show()
# Write Parsed Data to GCS in Parquet Format
# parsed_df.write.mode("overwrite").parquet("gs://test_bucket/parsed_payments/")
parsed_df.write.mode("overwrite").parquet("output/parsed_payments/")

spark.stop()