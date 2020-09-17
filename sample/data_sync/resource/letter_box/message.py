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
  body = data_sync.libs.utils.get_period_from_s3('letter_box_comment')
  period_from, period_to = data_sync.libs.utils.generate_time_period_from_json(body)
  return period_from, period_to

def save_period(period_from, period_to):
  body = data_sync.libs.utils.generate_json_from_time_period(period_from, period_to)
  data_sync.libs.utils.put_period_to_s3('letter_box_comment', body)

def extract(period_from, period_to):
  connection_name = data_sync.libs.constant.CONNECTION_LETTER_BOX
  database        = data_sync.libs.constant.DATABASE_LETTER_BOX
  query           = data_sync.libs.constant.QUERY_LETTER_BOX_MESSAGES.format(period_from=period_from, period_to=period_to)
  return data_sync.libs.utils.extract(connection_name, database, query, period_from, period_to)

def transform(dynamic_frame):
  application_name      = 'letter_box'
  resource_type         = 'comment'
  resource_name         = udf(lambda uuid: '/comments/%s' % uuid)
  related_resource_type = 'letter'
  related_resource_name = udf(lambda letter_uuid: '/letters/%s' % letter_uuid)
  review_texts          = udf(lambda text: json.dumps({ 'text': text }))
  to_localtime_str      = udf(lambda tt: (timezone('Asia/Tokyo').localize(tt) + timedelta(hours=9)).strftime('%Y-%m-%dT%H:%M:%S.%f%z'))
  dataframe = dynamic_frame.toDF()
  dataframe = dataframe.withColumn('application_name', lit(application_name))
  dataframe = dataframe.withColumn('resource_type', lit(resource_type))
  dataframe = dataframe.withColumn('resource_name', resource_name(dataframe['uuid']))
  dataframe = dataframe.withColumn('related_resource_type', lit(related_resource_type))
  dataframe = dataframe.withColumn('related_resource_name', related_resource_name(dataframe['letter_uuid']))
  dataframe = dataframe.withColumn('review_texts', review_texts(dataframe['text']))
  dataframe = dataframe.withColumn('reported_count', lit(0))
  dataframe = dataframe.withColumn('posted_at', to_localtime_str(dataframe['created_at']))
  dataframe = dataframe.withColumn('edited_at', to_localtime_str(dataframe['updated_at']))
  dataframe = dataframe.withColumn('finc_user_id', dataframe['finc_user_id'])
  return DynamicFrame.fromDF(dataframe, glueContext, 'letter_box_messages')

def select_fields(dynamic_frame):
  return data_sync.libs.utils.select_fields(dynamic_frame)

def load(dynamic_frame):
  data_sync.libs.utils.put_payload_to_s3('letter_box_comment', dynamic_frame)
