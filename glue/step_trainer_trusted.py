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

# Script generated for node step trainer
steptrainer_node1792205843927 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://wgu-d609-tklein2/landing/step_trainer/"], "recurse": True}, transformation_ctx="steptrainer_node1792205843927")

# Script generated for node curated customers
curatedcustomers_node1792206002061 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customers_curated", transformation_ctx="curatedcustomers_node1792206002061")

# Script generated for node SQL Query
SqlQuery0 = '''
SELECT
    s.sensorReadingTime,
    s.serialNumber,
    s.distanceFromObject
FROM step_trainer_source s
INNER JOIN (
    SELECT DISTINCT serialNumber
    FROM customer_source
) c
    ON s.serialNumber = c.serialNumber
'''
SQLQuery_node1792206042154 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"step_trainer_source":steptrainer_node1792205843927, "customer_source":curatedcustomers_node1792206002061}, transformation_ctx = "SQLQuery_node1792206042154")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SQLQuery_node1792206042154, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1792205997770", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1792206094374 = glueContext.getSink(path="s3://wgu-d609-tklein2/trusted/step_trainer/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1792206094374")
AmazonS3_node1792206094374.setCatalogInfo(catalogDatabase="stedi",catalogTableName="step_trainer_trusted")
AmazonS3_node1792206094374.setFormat("glueparquet", compression="snappy")
AmazonS3_node1792206094374.writeFrame(SQLQuery_node1792206042154)
job.commit()