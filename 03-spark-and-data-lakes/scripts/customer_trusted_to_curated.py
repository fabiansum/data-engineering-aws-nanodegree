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

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1724605505094 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_landing", transformation_ctx="AccelerometerLanding_node1724605505094")

# Script generated for node Customer Trusted
CustomerTrusted_node1724605524334 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_trusted", transformation_ctx="CustomerTrusted_node1724605524334")

# Script generated for node Join
Join_node1724605716939 = Join.apply(frame1=CustomerTrusted_node1724605524334, frame2=AccelerometerLanding_node1724605505094, keys1=["email"], keys2=["user"], transformation_ctx="Join_node1724605716939")

# Script generated for node Drop Fields and Duplicates
SqlQuery0 = '''
select distinct
    customerName,
    email,
    phone,
    birthDay,
    serialNumber,
    registrationDate,
    lastUpdateDate,
    shareWithResearchAsOfDate,
    shareWithPublicAsOfDate,
    shareWithFriendsAsOfDate
from
    myDataSource;
'''
DropFieldsandDuplicates_node1724605545184 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"myDataSource":Join_node1724605716939}, transformation_ctx = "DropFieldsandDuplicates_node1724605545184")

# Script generated for node Detect Sensitive Data
entity_detector = EntityDetector()
classified_map = entity_detector.classify_columns(DropFieldsandDuplicates_node1724605545184, ["EMAIL", "PERSON_NAME", "PHONE_NUMBER"], 1.0, 0.05, "HIGH")

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

DetectSensitiveData_node1724642630131 = hashDf(DropFieldsandDuplicates_node1724605545184, list(classified_map.keys()))

# Script generated for node Customer Curated
CustomerCurated_node1724605947060 = glueContext.write_dynamic_frame.from_options(frame=DetectSensitiveData_node1724642630131, connection_type="s3", format="json", connection_options={"path": "s3://stedi-lake-house-fs54/customer/curated/", "partitionKeys": []}, transformation_ctx="CustomerCurated_node1724605947060")

job.commit()