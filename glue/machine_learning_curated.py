import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
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

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node Trusted accelerometer
Trustedaccelerometer_node1792376662531 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_trusted", transformation_ctx="Trustedaccelerometer_node1792376662531")

# Script generated for node Trusted step trainer
Trustedsteptrainer_node1792376661050 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="step_trainer_trusted", transformation_ctx="Trustedsteptrainer_node1792376661050")

# Script generated for node SQL Query
SqlQuery0 = '''
SELECT
    s.sensorReadingTime AS reading_timestamp,
    s.distanceFromObject AS distance_from_object,
    a.x AS accelerometer_x,
    a.y AS accelerometer_y,
    a.z AS accelerometer_z
FROM step_trainer_source s
INNER JOIN accelerometer_source a
    ON s.sensorReadingTime = a.`timestamp`
'''
SQLQuery_node1792376726553 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"accelerometer_source":Trustedaccelerometer_node1792376662531, "step_trainer_source":Trustedsteptrainer_node1792376661050}, transformation_ctx = "SQLQuery_node1792376726553")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SQLQuery_node1792376726553, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1792376855181", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1792376883100 = glueContext.getSink(path="s3://wgu-d609-tklein2/curated/machine_learning/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1792376883100")
AmazonS3_node1792376883100.setCatalogInfo(catalogDatabase="stedi",catalogTableName="machine_learning_curated")
AmazonS3_node1792376883100.setFormat("glueparquet", compression="snappy")
AmazonS3_node1792376883100.writeFrame(SQLQuery_node1792376726553)
job.commit()