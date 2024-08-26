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

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1724606288269 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="step_trainer_trusted", transformation_ctx="StepTrainerTrusted_node1724606288269")

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1724607104340 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_trusted", transformation_ctx="AccelerometerTrusted_node1724607104340")

# Script generated for node Join
SqlQuery0 = '''
select distinct user, sensorreadingtime, serialnumber, distancefromobject, x, y, z
from stt
inner join at
on stt.sensorreadingtime = at.timestamp;
'''
Join_node1724606743342 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"stt":StepTrainerTrusted_node1724606288269, "at":AccelerometerTrusted_node1724607104340}, transformation_ctx = "Join_node1724606743342")

# Script generated for node Detect Sensitive Data
entity_detector = EntityDetector()
classified_map = entity_detector.classify_columns(Join_node1724606743342, ["EMAIL"], 1.0, 0.05, "HIGH")

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

DetectSensitiveData_node1724641348516 = hashDf(Join_node1724606743342, list(classified_map.keys()))

# Script generated for node Machine Learning Curated
MachineLearningCurated_node1724606792922 = glueContext.getSink(path="s3://stedi-lake-house-fs54/step_trainer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="MachineLearningCurated_node1724606792922")
MachineLearningCurated_node1724606792922.setCatalogInfo(catalogDatabase="stedi",catalogTableName="machine_learning_curated")
MachineLearningCurated_node1724606792922.setFormat("json")
MachineLearningCurated_node1724606792922.writeFrame(DetectSensitiveData_node1724641348516)
job.commit()