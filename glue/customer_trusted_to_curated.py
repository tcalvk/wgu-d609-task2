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

# Script generated for node trusted_customers
trusted_customers_node1792205136564 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_trusted", transformation_ctx="trusted_customers_node1792205136564")

# Script generated for node Trusted accelerometer
Trustedaccelerometer_node1792205181899 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_trusted", transformation_ctx="Trustedaccelerometer_node1792205181899")

# Script generated for node SQL Query
SqlQuery0 = '''
SELECT c.*
FROM customer_source c
INNER JOIN (
    SELECT DISTINCT `user`
    FROM accelerometer_source
) a
    ON c.email = a.`user`
'''
SQLQuery_node1792205227263 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"customer_source":trusted_customers_node1792205136564, "accelerometer_source":Trustedaccelerometer_node1792205181899}, transformation_ctx = "SQLQuery_node1792205227263")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SQLQuery_node1792205227263, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1792205078407", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1792205272932 = glueContext.getSink(path="s3://wgu-d609-tklein2/curated/customers/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1792205272932")
AmazonS3_node1792205272932.setCatalogInfo(catalogDatabase="stedi",catalogTableName="customers_curated")
AmazonS3_node1792205272932.setFormat("glueparquet", compression="snappy")
AmazonS3_node1792205272932.writeFrame(SQLQuery_node1792205227263)
job.commit()