import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node accelerometer_landing
accelerometer_landing_node1718641660690 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://baotcn/accelerometer/landing/"], "recurse": True}, transformation_ctx="accelerometer_landing_node1718641660690")

# Script generated for node customer_trusted
customer_trusted_node1718642040434 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://baotcn/customers/trusted/"], "recurse": True}, transformation_ctx="customer_trusted_node1718642040434")

# Script generated for node emails_join
emails_join_node1718641994642 = Join.apply(frame1=accelerometer_landing_node1718641660690, frame2=customer_trusted_node1718642040434, keys1=["user"], keys2=["email"], transformation_ctx="emails_join_node1718641994642")

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1718642196717 = glueContext.write_dynamic_frame.from_options(frame=emails_join_node1718641994642, connection_type="s3", format="json", connection_options={"path": "s3://baotcn/accelerometer/trusted/", "partitionKeys": []}, transformation_ctx="accelerometer_trusted_node1718642196717")

job.commit()