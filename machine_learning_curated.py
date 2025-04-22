import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue.dynamicframe import DynamicFrame
from awsglue import DynamicFrame
from pyspark.sql import functions as SqlFuncs

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

# Script generated for node customer landing
customerlanding_node1745300882916 = glueContext.create_dynamic_frame.from_catalog(database="default", table_name="customer_landing", transformation_ctx="customerlanding_node1745300882916")

# Script generated for node Step Trainer Landing
StepTrainerLanding_node1745301058672 = glueContext.create_dynamic_frame.from_catalog(database="default", table_name="step_trainer_landing", transformation_ctx="StepTrainerLanding_node1745301058672")

# Script generated for node SQL Query
SqlQuery0 = '''
select * from myDataSource

'''
SQLQuery_node1745301319375 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"mydatasource":customerlanding_node1745300882916, "mydatasource":StepTrainerLanding_node1745301058672}, transformation_ctx = "SQLQuery_node1745301319375")

# Script generated for node Drop Fields
DropFields_node1745301160647 = DropFields.apply(frame=SQLQuery_node1745301319375, paths=[], transformation_ctx="DropFields_node1745301160647")

# Script generated for node Drop Duplicates
DropDuplicates_node1745301240617 =  DynamicFrame.fromDF(DropFields_node1745301160647.toDF().dropDuplicates(), glueContext, "DropDuplicates_node1745301240617")

# Script generated for node Step trainer trusted
EvaluateDataQuality().process_rows(frame=DropDuplicates_node1745301240617, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1745300534370", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
Steptrainertrusted_node1745301400868 = glueContext.write_dynamic_frame.from_options(frame=DropDuplicates_node1745301240617, connection_type="s3", format="json", connection_options={"path": "s3://tanisha13-lake-house/step-trainer-landing/", "compression": "snappy", "partitionKeys": []}, transformation_ctx="Steptrainertrusted_node1745301400868")

job.commit()
