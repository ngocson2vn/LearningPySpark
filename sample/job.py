import sys
from datetime import datetime, timedelta

from pyspark.context import SparkContext
from pyspark.sql.types import StructType
from pyspark.sql.types import StructField
from pyspark.sql.types import StringType, IntegerType, FloatType, DoubleType, TimestampType, DateType
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrameWriter
from awsglue.utils import getResolvedOptions


logSchema = StructType([
  StructField('user_id', IntegerType(), False), 
  StructField('plan_id', IntegerType(), False),
  StructField('referrer', StringType(), False),
  StructField('view_name', StringType(), False),
  StructField('navigation_title', StringType(), False),
  StructField('params', StringType(), False),
  StructField('start_time', TimestampType(), False),
  StructField('end_time', TimestampType(), False),
  StructField('access_date', DateType(), False),
  StructField('max_scroll_position', DoubleType(), False),
  StructField('uuid', StringType(), False),
  StructField('timezone', StringType(), False),
  StructField('finc_user_id', IntegerType(), False),
  StructField('app_version', StringType(), False),
  StructField('app_region', StringType(), False),
  StructField('start_time_with_tz', StringType(), False),
  StructField('end_time_with_tz', StringType(), False),
])


def extract(row):
  from pyspark.sql import Row
  from datetime import timedelta
  results = []
  delta = (row['end_time'] - row['start_time']).total_seconds()
  for s in range(int(delta) + 1):
    active_at = row['start_time'] + timedelta(seconds=s)
    newRow = Row(finc_user_id=row['finc_user_id'], active_at=active_at)
    results.append(newRow)
  return results


glueContext = GlueContext(SparkContext.getOrCreate())
spark = glueContext.sparkSession

dt = datetime.utcnow() - timedelta(hours=1)
s3Path = 's3://view-access-logs-replication/{}/{:02d}/{:02d}/{:02d}/*'.format(dt.year, dt.month, dt.day, dt.hour)
logsDF = spark.read.option("delimiter", "|").schema(logSchema).csv(s3Path)
logsDF = logsDF.filter("finc_user_id > 0 AND start_time IS NOT NULL").distinct()

results = logsDF.rdd.flatMap(extract).toDF()
results = results.select("finc_user_id", "active_at")
results.persist()

logsDF = glueContext.create_dynamic_frame_from_rdd(results.rdd, "logsDF")
dfWriter = DynamicFrameWriter(glueContext)

connOptions = {
  "url": "jdbc:redshift://finc-analytics-2nd.crntx3nmgy9e.ap-northeast-1.redshift.amazonaws.com:5439/analytics", 
  "user": "analytics_batch", 
  "password": ANALYTICS_BATCH_PASSWORD, 
  "dbtable": "summary_tables.secondly_active_users_test", 
  "redshiftTmpDir": "s3://aws-glue-temporary-759549166074-ap-northeast-1/nguyen.son"
}

dfWriter.from_options(logsDF, connection_type='redshift', connection_options=connOptions)
