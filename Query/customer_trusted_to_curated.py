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

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1718643939320 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://baotcn/accelerometer/trusted/"], "recurse": True}, transformation_ctx="accelerometer_trusted_node1718643939320")

# Script generated for node customer_trusted
customer_trusted_node1718643704160 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://baotcn/customers/trusted/"], "recurse": True}, transformation_ctx="customer_trusted_node1718643704160")

# Script generated for node SQL Query
SqlQuery0 = '''
select customer.* from customer inner join accelerometer
on customer.email=accelerometer.email
'''
SQLQuery_node1718646756214 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"customer":customer_trusted_node1718643704160, "accelerometer":accelerometer_trusted_node1718643939320}, transformation_ctx = "SQLQuery_node1718646756214")

# Script generated for node Amazon S3
AmazonS3_node1718643943017 = glueContext.getSink(path="s3://baotcn/customers/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1718643943017")
AmazonS3_node1718643943017.setCatalogInfo(catalogDatabase="baotcn_db",catalogTableName="customer_trusted_to_curated_catalog ")
AmazonS3_node1718643943017.setFormat("json")
AmazonS3_node1718643943017.writeFrame(SQLQuery_node1718646756214)
job.commit()