import sys
import json
from urllib.request import Request, urlopen
from datetime import datetime

from pyspark.context import SparkContext
from pyspark.sql.types import StructType
from pyspark.sql.types import StructField
from pyspark.sql.types import StringType, IntegerType, FloatType, DoubleType, TimestampType, DateType
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrameWriter
from awsglue.utils import getResolvedOptions

SLACK_ERROR_URL = 'https://hooks.slack.com/services/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'
SLACK_INFOM_URL = 'https://hooks.slack.com/services/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'

ANALYTICS_BATCH_PASSWORD = getResolvedOptions(sys.argv, ['JOB_NAME', 'analytics_batch_password'])['analytics_batch_password']

class Slack:
  def __init__(self, *, url):
    self.url = url
    self.headers = {"Content-type": "application/json"}
    self.method = "POST"

  def post(self, **kwargs):
    data = json.dumps(kwargs).encode()
    req = Request(self.url, data=data, headers=self.headers, method=self.method)
    return urlopen(req).read().decode()


def notify(message):
  if message['type'] == 'INFO':
    slack_info = Slack(url=SLACK_INFOM_URL)
    slack_info.post(
      channel="#data_analytics_logs",
      text=message['text']
    )
  else:
    slack_error = Slack(url=SLACK_ERROR_URL)
    slack_error.post(text=message['text'])


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

try:
  glueContext = GlueContext(SparkContext.getOrCreate())
  # spark = SparkSession.builder.appName("update_secondly_active_users").config("spark.sql.broadcastTimeout", "3600").getOrCreate()
  spark = glueContext.sparkSession
  logsDF = spark.read.option("delimiter", "|").schema(logSchema).csv('s3://view-access-logs-replication/2020/04/20/*/*')
  logsDF = logsDF.filter("finc_user_id > 0 AND start_time IS NOT NULL").distinct()

  results = logsDF.rdd.flatMap(extract).toDF()
  results = results.select("finc_user_id", "active_at")

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
  notify({'type': 'INFO', 'text': '[{}] [Glue -> Redshift] UPDATE summary_tables.secondly_active_users_test successfully'.format(datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'))})
except Exception as ex:
  notify({'type': 'ERROR', 'text': '[{}] [Glue -> Redshift] Failed to update summary_tables.secondly_active_users_test\n```{}```'.format(datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'), ex)})
