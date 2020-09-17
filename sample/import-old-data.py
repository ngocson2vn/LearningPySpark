import sys
import json
from urllib.request import Request, urlopen
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

hour = '06'
logsDF = spark.read.option("delimiter", "|").schema(logSchema).csv('s3://view-access-logs-replication/2020/04/21/{}/*'.format(hour))
logsDF = logsDF.filter("finc_user_id > 0 AND start_time IS NOT NULL").distinct()

results = logsDF.rdd.flatMap(extract).toDF()
results = results.select("finc_user_id", "active_at")

parquetPath = "s3://data-lake-for-analytics/external_summary_tables/secondly_active_users/accessdate=2020-04-21/hour={}".format(hour)
parquetPath
results.write.parquet(parquetPath)

COPY summary_tables.secondly_active_users
FROM 's3://data-lake-for-analytics/external_summary_tables/secondly_active_users/accessdate=2020-04-21/hour=06'
IAM_ROLE 'arn:aws:iam::759549166074:role/finc-redshift-spectrum-role'
FORMAT AS PARQUET;