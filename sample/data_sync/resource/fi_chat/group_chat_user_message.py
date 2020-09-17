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
  body = data_sync.libs.utils.get_period_from_s3('group_chat_user_message')
  period_from, period_to = data_sync.libs.utils.generate_id_period_from_json(body)
  return period_from, period_to

def save_period(period_from, period_to):
  body = data_sync.libs.utils.generate_json_from_id_period(period_from, period_to)
  data_sync.libs.utils.put_period_to_s3('group_chat_user_message', body)

def extract(period_from, period_to):
  connection_name = data_sync.libs.constant.CONNECTION_FI_CHAT
  database        = data_sync.libs.constant.DATABASE_FI_CHAT
  query           = data_sync.libs.constant.QUERY_GROUP_CHAT_USER_MESSAGES.format(period_from=period_from, period_to=period_to)
  return data_sync.libs.utils.extract(connection_name, database, query, period_from, period_to)

def transform(dynamic_frame):
  application_name      = 'fi_chat'
  resource_type         = 'group_chat_user_message'
  resource_name         = udf(lambda chat_room_type, chat_room_id, postable_type, postable_id: '/chat_rooms/%s,%s/posts/%s,%s' % (chat_room_type, chat_room_id, postable_type, postable_id))
  related_resource_type = 'group_chat_user_message'
  related_resource_name = udf(lambda chat_room_type, chat_room_id: '/chat_rooms/%s,%s' % (chat_room_type, chat_room_id))
  review_texts          = udf(lambda message: json.dumps({ 'message': message }))
  to_localtime_str      = udf(lambda tt: (timezone('Asia/Tokyo').localize(tt) + timedelta(hours=9)).strftime('%Y-%m-%dT%H:%M:%S.%f%z'))
  dataframe = dynamic_frame.toDF()
  dataframe = dataframe.withColumn('application_name', lit(application_name))
  dataframe = dataframe.withColumn('resource_type', lit(resource_type))
  dataframe = dataframe.withColumn('resource_name', resource_name(lit('GroupChatRoom'), dataframe['group_chat_room_id'], lit('GroupChatUserMessage'), dataframe['uuid']))
  dataframe = dataframe.withColumn('related_resource_type', lit(related_resource_type))
  dataframe = dataframe.withColumn('related_resource_name', related_resource_name(lit('GroupChatRoom'), dataframe['group_chat_room_id']))
  dataframe = dataframe.withColumn('review_texts', review_texts(dataframe['message']))
  dataframe = dataframe.withColumn('posted_at', to_localtime_str(dataframe['created_at']))
  dataframe = dataframe.withColumn('edited_at', to_localtime_str(dataframe['updated_at']))
  dataframe = dataframe.withColumn('reported_count', lit(0))
  dataframe = dataframe.withColumn('user_id', dataframe['user_id']) # requl_mobile の user_id, ≠ finc_user_id
  return DynamicFrame.fromDF(dataframe, glueContext, 'group_chat_user_messages')

def get_last_id_from_dynamic_frame(dynamic_frame):
  return dynamic_frame.toDF().groupBy().max('id').first()['max(id)']

def select_fields(dynamic_frame):
  return data_sync.libs.utils.select_fields(dynamic_frame)

def load(dynamic_frame):
  data_sync.libs.utils.put_payload_to_s3('group_chat_user_message', dynamic_frame)
