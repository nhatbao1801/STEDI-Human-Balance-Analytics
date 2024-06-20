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
accelerometer_trusted_node1718649923317 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://baotcn/accelerometer/trusted/"], "recurse": True}, transformation_ctx="accelerometer_trusted_node1718649923317")

# Script generated for node step_trainer_trusted
step_trainer_trusted_node1718649888464 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://baotcn/step_trainer/trusted/"], "recurse": True}, transformation_ctx="step_trainer_trusted_node1718649888464")

# Script generated for node SQL Query
SqlQuery0 = '''
select * from step inner join acce
on step.sensorreadingtime = acce.timestamp
'''
SQLQuery_node1718650226212 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"step":step_trainer_trusted_node1718649888464, "acce":accelerometer_trusted_node1718649923317}, transformation_ctx = "SQLQuery_node1718650226212")

# Script generated for node Amazon S3
AmazonS3_node1718650568908 = glueContext.getSink(path="s3://baotcn/machine_learning/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1718650568908")
AmazonS3_node1718650568908.setCatalogInfo(catalogDatabase="baotcn_db",catalogTableName="machine_learning_catalog")
AmazonS3_node1718650568908.setFormat("json")
AmazonS3_node1718650568908.writeFrame(SQLQuery_node1718650226212)
job.commit()