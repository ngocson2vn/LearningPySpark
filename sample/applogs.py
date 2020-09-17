# +---------------------+-----------------------------+-------------+
# | Column              | Type                        | Modifiers   |
# |---------------------+-----------------------------+-------------|
# | user_id             | integer                     |  not null   |
# | plan_id             | integer                     |  not null   |
# | referrer            | character varying(256)      |             |
# | view_name           | character varying(256)      |             |
# | navigation_title    | character varying(256)      |             |
# | params              | character varying(256)      |             |
# | start_time          | timestamp without time zone |             |
# | end_time            | timestamp without time zone |             |
# | access_date         | date                        |             |
# | max_scroll_position | double precision            |             |
# | uuid                | character varying(256)      |             |
# | timezone            | character varying(20)       |             |
# | finc_user_id        | integer                     |             |
# | app_version         | character varying(20)       |             |
# | app_region          | character varying(20)       |             |
# | start_time_with_tz  | timestamp with time zone    |             |
# | end_time_with_tz    | timestamp with time zone    |             |
# +---------------------+-----------------------------+-------------+

# s3://aws-glue-scripts-759549166074-ap-northeast-1/nguyen.son/pythonlib/vendored.zip
# ssh glue@3.115.227.50 -t gluepyspark --conf "spark.pyspark.driver.python=/home/glue/.local/bin/bpython"

from pyspark.sql.types import StructType
from pyspark.sql.types import StructField
from pyspark.sql.types import StringType, IntegerType, FloatType, DoubleType, TimestampType, DateType

from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrameWriter

args = getResolvedOptions(sys.argv, ['ANALYTICS_BATCH_PASSWORD'])
ANALYTICS_BATCH_PASSWORD = args['ANALYTICS_BATCH_PASSWORD']

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

# logsDF = spark.read.option("delimiter", "|").schema(logSchema).csv('s3://view-access-logs-replication/2020/04/19/*/*')
logsDF = spark.read.option("delimiter", "|").schema(logSchema).csv('s3://view-access-logs-replication/2020/04/18/00/*')
logsDF = logsDF.filter("finc_user_id > 0 AND start_time IS NOT NULL").distinct()


def extract(row):
  from pyspark.sql import Row
  from datetime import timedelta
  results = []
  delta = (row['end_time'] - row['start_time']).total_seconds()
  for s in range(int(delta) + 1):
    t = row['start_time'] + timedelta(seconds=s)
    newRow = Row(finc_user_id=row['finc_user_id'], start_time=row['start_time'], end_time=row['end_time'], tick=t)
    results.append(newRow)
  return results

results = logsDF.rdd.flatMap(extract).toDF()
# parquetPath = "s3://data-lake-for-analytics/external_summary_tables/secondly_active_users/accessdate=2020-04-19"
# parquetPath = "s3://data-lake-for-analytics/external_summary_tables/secondly_active_users/accessdate=2020-04-18"
# results.write.parquet(parquetPath)


glueContext = GlueContext(SparkContext.getOrCreate())
logsDF = glueContext.create_dynamic_frame_from_rdd(results.rdd, "logsDF")
dfWriter = DynamicFrameWriter(glueContext)

connOptions = {
  "url": "jdbc:redshift://finc-analytics-2nd.crntx3nmgy9e.ap-northeast-1.redshift.amazonaws.com:5439/analytics", 
  "user": "analytics_batch", 
  "password": "F4jbDHcZ", 
  "dbtable": "summary_tables.secondly_active_users_tmp", 
  "redshiftTmpDir": "s3://aws-glue-temporary-759549166074-ap-northeast-1/nguyen.son"
}

dfWriter.from_options(logsDF, connection_type='redshift', connection_options=connOptions)

# s3://aws-glue-temporary-759549166074-ap-northeast-1/nguyen.son