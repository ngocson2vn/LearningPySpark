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

from datetime import datetime, timedelta
from pytz import timezone
import json

# ==============================================================================
# context

sparkContext = SparkContext.getOrCreate()
glueContext  = GlueContext(sparkContext)
sqlContext   = SQLContext(sparkContext)

# ==============================================================================
# function

def generate_period():
  body = data_sync.libs.utils.get_period_from_s3('try_comment')
  period_from, period_to = data_sync.libs.utils.generate_time_period_from_json(body)
  return period_from, period_to

def save_period(period_from, period_to):
  body = data_sync.libs.utils.generate_json_from_time_period(period_from, period_to)
  data_sync.libs.utils.put_period_to_s3('try_comment', body)

def extract(period_from, period_to):
  connection_name = data_sync.libs.constant.CONNECTION_REQUL_MOBILE
  database        = data_sync.libs.constant.DATABASE_REQUL_MOBILE
  query           = data_sync.libs.constant.QUERY_TRY_COMMENTS.format(period_from=period_from, period_to=period_to)
  return data_sync.libs.utils.extract(connection_name, database, query, period_from, period_to)

def transform(dynamic_frame):
  application_name      = 'requl_mobile'
  resource_type         = 'try_comment'
  resource_name         = udf(lambda content_type, content_id, id: '/try/indexeds/%s,%s/comments/%s' % (content_type, content_id, id))
  related_resource_type = 'try_indexed'
  related_resource_name = udf(lambda content_type, content_id: '/try/indexeds/%s,%s' % (content_type, content_id))
  review_texts          = udf(lambda comment: json.dumps({ 'comment': comment }))
  to_localtime_str      = udf(lambda tt: timezone('Asia/Tokyo').localize(tt).strftime('%Y-%m-%dT%H:%M:%S.%f%z'))
  dataframe = dynamic_frame.toDF()
  dataframe = dataframe.withColumn('application_name', lit(application_name))
  dataframe = dataframe.withColumn('resource_type', lit(resource_type))
  dataframe = dataframe.withColumn('resource_name', resource_name(dataframe['content_type'], dataframe['content_id'], dataframe['id']))
  dataframe = dataframe.withColumn('related_resource_type', lit(related_resource_type))
  dataframe = dataframe.withColumn('related_resource_name', related_resource_name(dataframe['content_type'], dataframe['content_id']))
  dataframe = dataframe.withColumn('review_texts', review_texts(dataframe['comment']))
  dataframe = dataframe.withColumn('reported_count', dataframe['reported_count'])
  dataframe = dataframe.withColumn('posted_at', to_localtime_str(dataframe['created_at']))
  dataframe = dataframe.withColumn('edited_at', to_localtime_str(dataframe['updated_at']))
  dataframe = dataframe.withColumn('finc_user_id', dataframe['finc_user_id'])
  return DynamicFrame.fromDF(dataframe, glueContext, 'try_comments')

def select_fields(dynamic_frame):
  return data_sync.libs.utils.select_fields(dynamic_frame)

def load(dynamic_frame):
  data_sync.libs.utils.put_payload_to_s3('try_comment', dynamic_frame)
