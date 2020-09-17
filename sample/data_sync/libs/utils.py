import data_sync.libs.constant

from awsglue.context       import GlueContext
from pyspark.context       import SparkContext
from pyspark.sql           import SQLContext

from awsglue.context       import GlueContext
from pyspark.context       import SparkContext
from pyspark.sql           import SQLContext

from awsglue.dynamicframe  import DynamicFrame

import boto3
from botocore.exceptions import ClientError

from datetime import datetime, timedelta
from pytz import timezone
from calendar import timegm
import json

# ==============================================================================
# context

sparkContext = SparkContext.getOrCreate()
glueContext  = GlueContext(sparkContext)
sqlContext   = SQLContext(sparkContext)

# ==============================================================================
# s3

s3_bucket      = data_sync.libs.constant.S3_BUCKET
s3_path_prefix = data_sync.libs.constant.S3_PATH_PREFIX

# ==============================================================================
# period

def get_period_from_s3(key):
  result = None
  try:
    path = '%s/period/%s' % (s3_path_prefix, key)
    result = boto3.resource('s3').Object(s3_bucket, path).get()['Body'].read().decode()
  except ClientError as e:
    return None
  return result

def put_period_to_s3(key, body):
  path = '%s/period/%s' % (s3_path_prefix, key)
  boto3.resource('s3').Object(s3_bucket, path).put(Body=body)

def generate_time_period_from_json(body):
  if body is None:
    period_from = datetime(1970, 1, 1, tzinfo=timezone('UTC'))
    period_to   = datetime.now(timezone('UTC')).replace(microsecond=0)
  else:
    _, period_from = map(lambda x: datetime.fromtimestamp(x, tz=timezone('UTC')), json.loads(body).values())
    period_to = datetime.now(timezone('UTC')).replace(microsecond=0)
  return period_from, period_to

def generate_json_from_time_period(period_from, period_to):
  body = {
    'period_from': timegm(period_from.utctimetuple()),
    'period_to': timegm(period_to.utctimetuple())
  }
  return json.dumps(body)

def generate_id_period_from_json(body):
  if body is None:
    period_from = 0
    period_to   = 9223372036854775807
  else:
    _, period_from = json.loads(body).values()
    period_to = 9223372036854775807
  return period_from, period_to

def generate_json_from_id_period(period_from, period_to):
  body = {
    'period_from': period_from,
    'period_to': period_to
  }
  return json.dumps(body)

# ==============================================================================
# initialize

# FIXME: DB接続に必要なライブラリをインポートするために必要。モジュールを特定する
def init_connection_driver():
  database = data_sync.libs.constant.DATABASE_ACTIVITY_TIMELINE
  glueContext.create_dynamic_frame.from_catalog(
    database   = database,
    table_name = "%s_%s" % (database, 'user_activities')
  )

def jdbc_options(connection_name, database, query):
  jdbc_conf = glueContext.extract_jdbc_conf(connection_name=connection_name)
  url       = (jdbc_conf['url'] + '/' + database)
  options = {
    "url":       url,
    "user":      jdbc_conf['user'],
    "password":  jdbc_conf['password'],
    "dbtable":   query,
  }
  return options

# ==============================================================================
# payload

def extract(connection_name, database, query, period_from, period_to):
  options = jdbc_options(
    connection_name = connection_name,
    database        = database,
    query           = query,
  )
  dataframe = sqlContext.read.format('jdbc').options(**options).load()
  return DynamicFrame.fromDF(dataframe, glueContext, 'frame')

def select_fields(dynamic_frame):
  return dynamic_frame.select_fields(
    ['application_name', 'resource_type', 'resource_name', 'related_resource_type', 'related_resource_name', 'review_texts', 'reported_count', 'posted_at', 'edited_at', 'finc_user_id']
  )

def put_payload_to_s3(key, dynamic_frame):
  version = datetime.now().strftime('%Y_%m_%d_%H_%M_%S')
  path = '%s/payloads/%s_%s' % (s3_path_prefix, version, key)
  glueContext.write_dynamic_frame.from_options(
    frame              = dynamic_frame,
    connection_type    = "s3",
    connection_options = { "path": "s3://%s/%s" % (s3_bucket, path) },
    format             = "json"
  )

# ==============================================================================
# conversion_table

def extract_conversion_table_from_s3(key):
  path = 's3://%s/%s/conversion_tables/%s' % (s3_bucket, s3_path_prefix, key)
  dynamic_frame = glueContext.create_dynamic_frame_from_options(
    's3',
    { 'paths': [path], 'recurse': True, 'groupFiles': 'inPartition', 'groupSize': 1024 * 1024 },
    format="json",
  )
  return dynamic_frame

def put_conversion_table_to_s3(key, dynamic_frame):
  path = 's3://%s/%s/conversion_tables/%s' % (s3_bucket, s3_path_prefix, key)
  glueContext.spark_session.conf.set('spark.sql.sources.partitionOverwriteMode', 'dynamic')
  dynamic_frame.toDF().write.mode('overwrite').partitionBy('created_on').json(path)
  glueContext.spark_session.conf.set('spark.sql.sources.partitionOverwriteMode', 'static')
