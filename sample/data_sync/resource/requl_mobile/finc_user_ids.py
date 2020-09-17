import data_sync.libs.constant
import data_sync.libs.utils

from awsglue.context       import GlueContext
from pyspark.context       import SparkContext
from pyspark.sql           import SQLContext

from awsglue.dynamicframe  import DynamicFrame
from awsglue.transforms    import *
from pyspark.sql.functions import lit
from pyspark.sql.functions import udf
from pyspark.sql.functions import concat
from pyspark.sql.functions import format_string

import boto3
from botocore.exceptions import ClientError
from datetime import datetime, time, timedelta
from pytz import timezone
import json

# ==============================================================================
# context

sparkContext = SparkContext.getOrCreate()
glueContext  = GlueContext(sparkContext)
sqlContext   = SQLContext(sparkContext)

# ==============================================================================
# 前回実行日から現在までに作成されたアカウントを取得する
# 日付ごとにパーティション分割し、更新のあったパーティションは上書き更新を行う

# `created_at BETWEEN period_from AND period_to` で抽出する
# period_from / 前回実行日の`00:00:00`
# period_to   / 翌日の`00:00:00`
def generate_period():
  body = data_sync.libs.utils.get_period_from_s3('finc_user_ids')
  period_from, period_to = data_sync.libs.utils.generate_time_period_from_json(body)
  # 前回実行日当日を返す必要がある(翌日の`00:00:00`が保存されているため一日マイナスする)
  period_from = timezone('UTC').localize(datetime.combine(period_from.date(), time())) - timedelta(days = 1)
  period_to   = timezone('UTC').localize(datetime.combine(period_to + timedelta(days = 1), time()))
  return period_from, period_to

def save_period(period_from, period_to):
  body = data_sync.libs.utils.generate_json_from_time_period(period_from, period_to)
  data_sync.libs.utils.put_period_to_s3('finc_user_ids', body)

def extract(period_from, period_to):
  connection_name = data_sync.libs.constant.CONNECTION_REQUL_MOBILE
  database        = data_sync.libs.constant.DATABASE_REQUL_MOBILE
  query           = data_sync.libs.constant.QUERY_FINC_USER_ID.format(period_from=period_from, period_to=period_to)
  return data_sync.libs.utils.extract(connection_name, database, query, period_from, period_to)

def transform(dynamic_frame):
  return dynamic_frame

def select_fields(dynamic_frame):
  return dynamic_frame.select_fields(
    ['created_on', 'user_id', 'finc_user_id']
  )

def load(dynamic_frame):
  data_sync.libs.utils.put_conversion_table_to_s3('finc_user_id', dynamic_frame)

def join_finc_user_id(on_key, dynamic_frame):
  finc_user_ids = data_sync.libs.utils.extract_conversion_table_from_s3('finc_user_id')
  return Join.apply(dynamic_frame, finc_user_ids, on_key, on_key)
