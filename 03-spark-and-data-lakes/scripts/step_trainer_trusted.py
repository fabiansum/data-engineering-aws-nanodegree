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

# Script generated for node Step Trainer Landing
StepTrainerLanding_node1724600922238 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="step_trainer_landing", transformation_ctx="StepTrainerLanding_node1724600922238")

# Script generated for node Customer Curated
CustomerCurated_node1724600997060 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_curated", transformation_ctx="CustomerCurated_node1724600997060")

# Script generated for node SQL Query
SqlQuery0 = '''
select stl.sensorreadingtime, stl.serialnumber, stl.distancefromobject
from cc
inner join stl 
on cc.serialnumber = stl.serialnumber;
'''
SQLQuery_node1724601042981 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"cc":CustomerCurated_node1724600997060, "stl":StepTrainerLanding_node1724600922238}, transformation_ctx = "SQLQuery_node1724601042981")

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1724601306237 = glueContext.getSink(path="s3://stedi-lake-house-fs54/step_trainer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="StepTrainerTrusted_node1724601306237")
StepTrainerTrusted_node1724601306237.setCatalogInfo(catalogDatabase="stedi",catalogTableName="step_trainer_trusted")
StepTrainerTrusted_node1724601306237.setFormat("json")
StepTrainerTrusted_node1724601306237.writeFrame(SQLQuery_node1724601042981)
job.commit()