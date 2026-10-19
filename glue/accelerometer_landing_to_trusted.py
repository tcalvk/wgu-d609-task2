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

# Script generated for node Amazon S3
AmazonS3_node1792204338990 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://wgu-d609-tklein2/landing/accelerometer/"], "recurse": True}, transformation_ctx="AmazonS3_node1792204338990")

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1792204366874 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_trusted", transformation_ctx="AWSGlueDataCatalog_node1792204366874")

# Script generated for node SQL Query
SqlQuery0 = '''
SELECT
    a.`timestamp`,
    a.`user`,
    a.x,
    a.y,
    a.z
FROM accelerometer_source a
INNER JOIN customer_source c
    ON a.`user` = c.email
   AND a.`timestamp` >= c.shareWithResearchAsOfDate
'''
SQLQuery_node1792204402917 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"customer_source":AWSGlueDataCatalog_node1792204366874, "accelerometer_source":AmazonS3_node1792204338990}, transformation_ctx = "SQLQuery_node1792204402917")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SQLQuery_node1792204402917, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1792203920144", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1792204449765 = glueContext.getSink(path="s3://wgu-d609-tklein2/trusted/accelerometer/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1792204449765")
AmazonS3_node1792204449765.setCatalogInfo(catalogDatabase="stedi",catalogTableName="accelerometer_trusted")
AmazonS3_node1792204449765.setFormat("glueparquet", compression="snappy")
AmazonS3_node1792204449765.writeFrame(SQLQuery_node1792204402917)
job.commit()