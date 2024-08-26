import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglueml.transforms import EntityDetector
from pyspark.sql.types import StringType
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import *
from awsglue import DynamicFrame
import hashlib

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1724643213413 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_trusted", transformation_ctx="AccelerometerTrusted_node1724643213413")

# Script generated for node Customer Trusted
CustomerTrusted_node1724643254453 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_trusted", transformation_ctx="CustomerTrusted_node1724643254453")

# Script generated for node Join
Join_node1724643367967 = Join.apply(frame1=AccelerometerTrusted_node1724643213413, frame2=CustomerTrusted_node1724643254453, keys1=["user"], keys2=["email"], transformation_ctx="Join_node1724643367967")

# Script generated for node Filter
SqlQuery0 = '''
select user, timestamp, x, y, z from myDataSource
where timestamp >= shareWithResearchAsOfDate
'''
Filter_node1724643419974 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"myDataSource":Join_node1724643367967}, transformation_ctx = "Filter_node1724643419974")

# Script generated for node Detect Sensitive Data
entity_detector = EntityDetector()
classified_map = entity_detector.classify_columns(Filter_node1724643419974, ["EMAIL"], 1.0, 0.05, "HIGH")

def pii_column_hash(original_cell_value):
    return hashlib.sha256(str(original_cell_value).encode()).hexdigest()

pii_column_hash_udf = udf(pii_column_hash, StringType())

def hashDf(df, keys):
    if not keys:
        return df
    df_to_hash = df.toDF()
    for key in keys:
        df_to_hash = df_to_hash.withColumn(key, pii_column_hash_udf(key))
    return DynamicFrame.fromDF(df_to_hash, glueContext, "updated_hashed_df")

DetectSensitiveData_node1724643841503 = hashDf(Filter_node1724643419974, list(classified_map.keys()))

# Script generated for node Amazon S3
AmazonS3_node1724643830252 = glueContext.getSink(path="s3://stedi-lake-house-fs54/accelerometer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1724643830252")
AmazonS3_node1724643830252.setCatalogInfo(catalogDatabase="stedi",catalogTableName="accelerometer_curated")
AmazonS3_node1724643830252.setFormat("json")
AmazonS3_node1724643830252.writeFrame(DetectSensitiveData_node1724643841503)
job.commit()