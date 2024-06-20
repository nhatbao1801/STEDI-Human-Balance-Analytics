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

# Script generated for node step_trainer_landing
step_trainer_landing_node1718647833686 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://baotcn/step_trainer/landing/"], "recurse": True}, transformation_ctx="step_trainer_landing_node1718647833686")

# Script generated for node customer_curated
customer_curated_node1718647839609 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://baotcn/customers/curated/"], "recurse": True}, transformation_ctx="customer_curated_node1718647839609")

# Script generated for node SQL Query
SqlQuery0 = '''
select * from step inner join (select distinct * from cus) cus_dis
on step.serialnumber=cus_dis.serialnumber
'''
SQLQuery_node1718648689475 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"step":step_trainer_landing_node1718647833686, "cus":customer_curated_node1718647839609}, transformation_ctx = "SQLQuery_node1718648689475")

# Script generated for node Amazon S3
AmazonS3_node1718647842139 = glueContext.getSink(path="s3://baotcn/step_trainer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1718647842139")
AmazonS3_node1718647842139.setCatalogInfo(catalogDatabase="baotcn_db",catalogTableName="step_trainer trusted_catalog")
AmazonS3_node1718647842139.setFormat("json")
AmazonS3_node1718647842139.writeFrame(SQLQuery_node1718648689475)
job.commit()