import sys
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql import SQLContext

import data_sync.libs.utils

import data_sync.resource.activity_timeline.snap
import data_sync.resource.activity_timeline.snap_comment
import data_sync.resource.requl_mobile.finc_user_ids
import data_sync.resource.requl_mobile.try_comment
import data_sync.resource.letter_box.message
import data_sync.resource.fi_chat.chat_direct_message
import data_sync.resource.fi_chat.group_chat_user_message

# ============================================================================

sparkContext = SparkContext.getOrCreate()
glueContext  = GlueContext(sparkContext)
job          = Job(glueContext)

# ============================================================================

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'STAGE'])
job.init(args['JOB_NAME'], args)

stage = args['STAGE']

# FIXME: DB接続に必要なライブラリのロードに必要
data_sync.libs.utils.init_connection_driver()

# finc_user_id
period_from, period_to = data_sync.resource.requl_mobile.finc_user_ids.generate_period()
frames = data_sync.resource.requl_mobile.finc_user_ids.extract(period_from, period_to)
frames = data_sync.resource.requl_mobile.finc_user_ids.transform(frames)
frames = data_sync.resource.requl_mobile.finc_user_ids.select_fields(frames)
data_sync.resource.requl_mobile.finc_user_ids.load(frames)
data_sync.resource.requl_mobile.finc_user_ids.save_period(period_from, period_to)

# snap
period_from, period_to = data_sync.resource.activity_timeline.snap.generate_period()
frames = data_sync.resource.activity_timeline.snap.extract(period_from, period_to)
frames = data_sync.resource.activity_timeline.snap.transform(frames)
frames = data_sync.resource.activity_timeline.snap.select_fields(frames)
data_sync.resource.activity_timeline.snap.load(frames)
data_sync.resource.activity_timeline.snap.save_period(period_from, period_to)

# snap_comment
period_from, period_to = data_sync.resource.activity_timeline.snap_comment.generate_period()
frames = data_sync.resource.activity_timeline.snap_comment.extract(period_from, period_to)
frames = data_sync.resource.activity_timeline.snap_comment.transform(frames)
frames = data_sync.resource.activity_timeline.snap_comment.select_fields(frames)
data_sync.resource.activity_timeline.snap_comment.load(frames)
data_sync.resource.activity_timeline.snap_comment.save_period(period_from, period_to)

# try_comment
period_from, period_to = data_sync.resource.requl_mobile.try_comment.generate_period()
frames = data_sync.resource.requl_mobile.try_comment.extract(period_from, period_to)
frames = data_sync.resource.requl_mobile.try_comment.transform(frames)
frames = data_sync.resource.requl_mobile.try_comment.select_fields(frames)
data_sync.resource.requl_mobile.try_comment.load(frames)
data_sync.resource.requl_mobile.try_comment.save_period(period_from, period_to)

# letter_box
period_from, period_to = data_sync.resource.letter_box.message.generate_period()
frames = data_sync.resource.letter_box.message.extract(period_from, period_to)
frames = data_sync.resource.letter_box.message.transform(frames)
frames = data_sync.resource.letter_box.message.select_fields(frames)
data_sync.resource.letter_box.message.load(frames)
data_sync.resource.letter_box.message.save_period(period_from, period_to)

# TODO: FiChatの本番同期が開始したら削除する
if stage != 'production':
  # chat_direct_message
  period_from, period_to = data_sync.resource.fi_chat.chat_direct_message.generate_period()
  frames = data_sync.resource.fi_chat.chat_direct_message.extract(period_from, period_to)
  frames = data_sync.resource.fi_chat.chat_direct_message.transform(frames)
  last_id = data_sync.resource.fi_chat.chat_direct_message.get_last_id_from_dynamic_frame(frames)
  last_id = period_from if last_id is None else last_id
  frames = data_sync.resource.requl_mobile.finc_user_ids.join_finc_user_id('user_id', frames)
  frames = data_sync.resource.fi_chat.chat_direct_message.select_fields(frames)
  data_sync.resource.fi_chat.chat_direct_message.load(frames)
  data_sync.resource.fi_chat.chat_direct_message.save_period(period_from, last_id)

  # group_chat_user_message
  period_from, period_to = data_sync.resource.fi_chat.group_chat_user_message.generate_period()
  frames = data_sync.resource.fi_chat.group_chat_user_message.extract(period_from, period_to)
  frames = data_sync.resource.fi_chat.group_chat_user_message.transform(frames)
  last_id = data_sync.resource.fi_chat.group_chat_user_message.get_last_id_from_dynamic_frame(frames)
  last_id = period_from if last_id is None else last_id
  frames = data_sync.resource.requl_mobile.finc_user_ids.join_finc_user_id('user_id', frames)
  frames = data_sync.resource.fi_chat.group_chat_user_message.select_fields(frames)
  data_sync.resource.fi_chat.group_chat_user_message.load(frames)
  data_sync.resource.fi_chat.group_chat_user_message.save_period(period_from, last_id)

# commit
job.commit()
