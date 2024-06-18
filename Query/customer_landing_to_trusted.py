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

# Script generated for node customer_landing
customer_landing_node1718639695744 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://baotcn/customers/"], "recurse": True}, transformation_ctx="customer_landing_node1718639695744")

# Script generated for node SQL Query
SqlQuery0 = '''
select * from myDataSource
where myDataSource.shareWithResearchAsOfDate is not null
'''
SQLQuery_node1718640489603 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"myDataSource":customer_landing_node1718639695744}, transformation_ctx = "SQLQuery_node1718640489603")

# Script generated for node customer_trusted
customer_trusted_node1718639855460 = glueContext.write_dynamic_frame.from_options(frame=SQLQuery_node1718640489603, connection_type="s3", format="json", connection_options={"path": "s3://baotcn/customers/trusted/", "partitionKeys": []}, transformation_ctx="customer_trusted_node1718639855460")

job.commit()