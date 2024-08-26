import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame

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
AccelerometerLanding_node1724602275407 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_landing", transformation_ctx="AccelerometerLanding_node1724602275407")

# Script generated for node Customer Trusted
CustomerTrusted_node1724602395733 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_trusted", transformation_ctx="CustomerTrusted_node1724602395733")

# Script generated for node Join
SqlQuery0 = '''
select al.user, al.timestamp, al.x, al.y, al.z
from al
INNER JOIN ct 
ON al.user = ct.email;

'''
Join_node1724602419520 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"al":AccelerometerLanding_node1724602275407, "ct":CustomerTrusted_node1724602395733}, transformation_ctx = "Join_node1724602419520")

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1724602461059 = glueContext.getSink(path="s3://stedi-lake-house-fs54/accelerometer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AccelerometerTrusted_node1724602461059")
AccelerometerTrusted_node1724602461059.setCatalogInfo(catalogDatabase="stedi",catalogTableName="accelerometer_trusted")
AccelerometerTrusted_node1724602461059.setFormat("json")
AccelerometerTrusted_node1724602461059.writeFrame(Join_node1724602419520)
job.commit()