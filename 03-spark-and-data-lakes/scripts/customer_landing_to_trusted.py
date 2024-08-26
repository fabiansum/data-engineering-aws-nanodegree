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

# Script generated for node Customer Landing
CustomerLanding_node1724603469328 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_landing", transformation_ctx="CustomerLanding_node1724603469328")

# Script generated for node Share with Research
SqlQuery0 = '''
select * from cl
where shareWithResearchAsOfDate is not null;
'''
SharewithResearch_node1724603492060 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"cl":CustomerLanding_node1724603469328}, transformation_ctx = "SharewithResearch_node1724603492060")

# Script generated for node Customer Trusted
CustomerTrusted_node1724603630506 = glueContext.getSink(path="s3://stedi-lake-house-fs54/customer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="CustomerTrusted_node1724603630506")
CustomerTrusted_node1724603630506.setCatalogInfo(catalogDatabase="stedi",catalogTableName="customer_trusted")
CustomerTrusted_node1724603630506.setFormat("json")
CustomerTrusted_node1724603630506.writeFrame(SharewithResearch_node1724603492060)
job.commit()