import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col

# Initialize Glue context
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'source_bucket', 'source_key'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Read the CSV file from S3
source_bucket = args['source_bucket']
source_key = args['source_key']
input_path = f's3://{source_bucket}/{source_key}'

df = spark.read.format('csv').option('header', 'true').load(input_path)

# Debugging: Print schema and show some rows
print("Schema of the DataFrame:")
df.printSchema()

print("Showing some rows from the DataFrame:")
df.show(5)

# Cast columns to appropriate data types
df = df.withColumn("TransactionAmount", col("TransactionAmount").cast("double")) \
       .withColumn("AccountBalance", col("AccountBalance").cast("double")) \
       .withColumn("CustomerAge", col("CustomerAge").cast("integer")) \
       .withColumn("CustomerIncome", col("CustomerIncome").cast("double")) \
       .withColumn("FraudFlag", col("FraudFlag").cast("boolean"))

# Fill null values in merchantcode with 'n/a'
df = df.fillna({"merchantcode": "n/a"})

# Debugging: Print schema and show some rows after casting
print("Schema of the DataFrame after casting and filling nulls:")
df.printSchema()

print("Showing some rows from the DataFrame after casting and filling nulls:")
df.show(5)

# Write the transformed data to RDS PostgreSQL
connection_options = {
    "url": "jdbc:postgresql://my-postgres-db-usecase.cjsw6wac29av.us-east-1.rds.amazonaws.com:5432/mydatabase",
    "dbtable": "transactions_test",
    "user": "SanjayramAdmin",
    "password": "Akash$1234",
    "customJdbcDriverS3Path": "s3://source-bucket-usecase/scripts/postgresql-42.2.19.jar",
    "customJdbcDriverClassName": "org.postgresql.Driver"
}

df.write \
    .format("jdbc") \
    .option("url", connection_options["url"]) \
    .option("dbtable", connection_options["dbtable"]) \
    .option("user", connection_options["user"]) \
    .option("password", connection_options["password"]) \
    .option("driver", connection_options["customJdbcDriverClassName"]) \
    .mode("append") \
    .save()

job.commit()


