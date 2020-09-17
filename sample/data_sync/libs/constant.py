import sys
import awsglue
from awsglue.utils import getResolvedOptions
from awsglue.utils import GlueArgumentError

try:
  args = getResolvedOptions(sys.argv, ['STAGE'])
  stage = args['STAGE']
except GlueArgumentError:
  stage = 'development'

if stage == 'development':
  S3_BUCKET      = 'cgm-patrol-staging'
  S3_PATH_PREFIX = 'data_sync/development/v2'

  CONNECTION_ACTIVITY_TIMELINE = 'activity_timeline_staging_con'
  CONNECTION_REQUL_MOBILE      = 'requl_mobile_staging_con'
  CONNECTION_FI_CHAT           = 'fi_chat_staging_con'
  CONNECTION_LETTER_BOX        = 'letter_box_staging_con'

  DATABASE_ACTIVITY_TIMELINE   = 'activity_timeline_staging'
  DATABASE_REQUL_MOBILE        = 'requl_mobile_staging'
  DATABASE_FI_CHAT             = 'fi_chat_staging'
  DATABASE_LETTER_BOX          = 'letter_box_staging'
elif stage == 'staging':
  S3_BUCKET      = 'cgm-patrol-staging'
  S3_PATH_PREFIX = 'data_sync/staging/v2'

  CONNECTION_ACTIVITY_TIMELINE = 'activity_timeline_staging_con'
  CONNECTION_REQUL_MOBILE      = 'requl_mobile_staging_con'
  CONNECTION_FI_CHAT           = 'fi_chat_staging_con'
  CONNECTION_LETTER_BOX        = 'letter_box_staging_con'

  DATABASE_ACTIVITY_TIMELINE   = 'activity_timeline_staging'
  DATABASE_REQUL_MOBILE        = 'requl_mobile_staging'
  DATABASE_FI_CHAT             = 'fi_chat_staging'
  DATABASE_LETTER_BOX          = 'letter_box_staging'
elif stage == 'production':
  S3_BUCKET      = 'cgm-patrol-production'
  S3_PATH_PREFIX = 'data_sync/production/v2'

  CONNECTION_ACTIVITY_TIMELINE = 'activity_timeline_production_con'
  CONNECTION_REQUL_MOBILE      = 'requl_mobile_production_con'
  CONNECTION_FI_CHAT           = 'fi_chat_production_con'
  CONNECTION_LETTER_BOX        = 'letter_box_production_con'

  DATABASE_ACTIVITY_TIMELINE   = 'activity_timeline_production'
  DATABASE_REQUL_MOBILE        = 'requl_mobile_production'
  DATABASE_FI_CHAT             = 'fi_chat_production'
  DATABASE_LETTER_BOX          = 'letter_box_production'
else:
  raise ValueError('unknown stage is given')

QUERY_SNAPS = '''
  (
    SELECT t1_alias.id, t1_alias.title, t1_alias.description, t1_alias.message, t1_alias.created_at, t1_alias.updated_at, t1_alias.acted_finc_user_id, IFNULL(t2_alias.reported_count, 0) AS reported_count
    FROM (
      SELECT
        *
      FROM
        user_activities
      WHERE
          (
            activity_type = 401
          AND
            '{period_from}' <= updated_at AND updated_at <= '{period_to}'
          )
        OR
          (
            id IN (
              SELECT
                DISTINCT problem_reportable_id
              FROM
                problem_reports
              INNER JOIN user_activities ON problem_reports.problem_reportable_id = user_activities.id
              WHERE
                problem_reportable_type = 'UserActivity'
              AND
                activity_type = 401
              AND
                '{period_from}' <= problem_reports.updated_at AND problem_reports.updated_at <= '{period_to}'
            )
          )
    ) t1_alias
    LEFT OUTER JOIN (
      SELECT
        problem_reportable_id, COUNT(problem_reportable_id) AS reported_count
      FROM
        problem_reports
      INNER JOIN user_activities ON problem_reports.problem_reportable_id = user_activities.id
      WHERE
        problem_reportable_type = 'UserActivity'
      AND
        activity_type = 401
      GROUP BY
        problem_reportable_type, problem_reportable_id
    ) t2_alias ON t1_alias.id = t2_alias.problem_reportable_id
  ) t3_alias
'''

QUERY_SNAP_COMMENTS = '''
  (
    SELECT t1_alias.id, t1_alias.user_activity_id, t1_alias.comment, t1_alias.created_at, t1_alias.updated_at, t1_alias.finc_user_id, IFNULL(t2_alias.reported_count, 0) AS reported_count
    FROM (
      SELECT
        *
      FROM
        comments
      WHERE
        '{period_from}' <= updated_at AND updated_at <= '{period_to}'
       OR
        id IN (
          SELECT
            DISTINCT problem_reportable_id
          FROM
            problem_reports
          WHERE
            problem_reportable_type = 'Comment'
           AND
            '{period_from}' <= updated_at AND updated_at <= '{period_to}'
        )
    ) t1_alias
    LEFT OUTER JOIN (
      SELECT
        problem_reportable_id, COUNT(problem_reportable_id) AS reported_count
      FROM
        problem_reports
      WHERE
        problem_reportable_type = 'Comment'
      GROUP BY
        problem_reportable_type, problem_reportable_id
    ) t2_alias ON t1_alias.id = t2_alias.problem_reportable_id
  ) t3_alias
'''

QUERY_TRY_INDEXEDS  = '''
  (
    SELECT content_type, content_id, original_created_at, original_updated_at, title, description, details, users.finc_user_id
    FROM (
      SELECT content_type, content_id, original_created_at, original_updated_at, try_indexeds.user_id, title, description, details FROM try_indexeds
      INNER JOIN try_beauties ON try_indexeds.content_type = 'Try::Beauty' AND try_indexeds.content_id = try_beauties.id
      WHERE '{period_from}' <= original_updated_at AND original_updated_at <= '{period_to}'
      UNION ALL
      SELECT content_type, content_id, original_created_at, original_updated_at, try_indexeds.user_id, title, description, details FROM try_indexeds
      INNER JOIN try_cools ON try_indexeds.content_type = 'Try::Cool' AND try_indexeds.content_id = try_cools.id
      WHERE '{period_from}' <= original_updated_at AND original_updated_at <= '{period_to}'
      UNION ALL
      SELECT content_type, content_id, original_created_at, original_updated_at, try_indexeds.user_id, title, description, details FROM try_indexeds
      INNER JOIN try_events ON try_indexeds.content_type = 'Try::Event' AND try_indexeds.content_id = try_events.id
      WHERE '{period_from}' <= original_updated_at AND original_updated_at <= '{period_to}'
      UNION ALL
      SELECT content_type, content_id, original_created_at, original_updated_at, try_indexeds.user_id, title, description, details FROM try_indexeds
      INNER JOIN try_fitnesses ON try_indexeds.content_type = 'Try::Fitness' AND try_indexeds.content_id = try_fitnesses.id
      WHERE '{period_from}' <= original_updated_at AND original_updated_at <= '{period_to}'
      UNION ALL
      SELECT content_type, content_id, original_created_at, original_updated_at, try_indexeds.user_id, title, description, details FROM try_indexeds
      INNER JOIN try_gourmets ON try_indexeds.content_type = 'Try::Gourmet' AND try_indexeds.content_id = try_gourmets.id
      WHERE '{period_from}' <= original_updated_at AND original_updated_at <= '{period_to}'
      UNION ALL
      SELECT content_type, content_id, original_created_at, original_updated_at, try_indexeds.user_id, title, description, details FROM try_indexeds
      INNER JOIN try_life_hacks ON try_indexeds.content_type = 'Try::LifeHack' AND try_indexeds.content_id = try_life_hacks.id
      WHERE '{period_from}' <= original_updated_at AND original_updated_at <= '{period_to}'
      UNION ALL
      SELECT content_type, content_id, original_created_at, original_updated_at, try_indexeds.user_id, title, description, details FROM try_indexeds
      INNER JOIN try_recipes ON try_indexeds.content_type = 'Try::Recipe' AND try_indexeds.content_id = try_recipes.id
      WHERE '{period_from}' <= original_updated_at AND original_updated_at <= '{period_to}'
      UNION ALL
      SELECT content_type, content_id, original_created_at, original_updated_at, try_indexeds.user_id, title, description, details FROM try_indexeds
      INNER JOIN try_shops ON try_indexeds.content_type = 'Try::Shop' AND try_indexeds.content_id = try_shops.id
      WHERE '{period_from}' <= original_updated_at AND original_updated_at <= '{period_to}'
      UNION ALL
      SELECT content_type, content_id, original_created_at, original_updated_at, try_indexeds.user_id, title, description, details FROM try_indexeds
      INNER JOIN try_styles ON try_indexeds.content_type = 'Try::Style' AND try_indexeds.content_id = try_styles.id
      WHERE '{period_from}' <= original_updated_at AND original_updated_at <= '{period_to}'
      UNION ALL
      SELECT content_type, content_id, original_created_at, original_updated_at, try_indexeds.user_id, title, description, details FROM try_indexeds
      INNER JOIN try_walking_events ON try_indexeds.content_type = 'Try::WalkingEvent' AND try_indexeds.content_id = try_walking_events.id
      WHERE '{period_from}' <= original_updated_at AND original_updated_at <= '{period_to}'
    ) t1_alias
    INNER JOIN users ON t1_alias.user_id = users.id
  ) t2_alias
'''

QUERY_TRY_COMMENTS  = '''
  (
    SELECT t1_alias.id, content_type, content_id, comment, t1_alias.created_at, t1_alias.updated_at, users.finc_user_id, IFNULL(t2_alias.reported_count, 0) AS reported_count
    FROM (
      SELECT
        *,
        CASE
          WHEN try_beauty_id        IS NOT NULL THEN 'Try::Beauty'
          WHEN try_fitness_id       IS NOT NULL THEN 'Try::Fitness'
          WHEN try_gourmet_id       IS NOT NULL THEN 'Try::Gourmet'
          WHEN try_life_hack_id     IS NOT NULL THEN 'Try::LifeHack'
          WHEN try_recipe_id        IS NOT NULL THEN 'Try::Recipe'
          WHEN try_style_id         IS NOT NULL THEN 'Try::Style'
          WHEN try_cool_id          IS NOT NULL THEN 'Try::Cool'
          WHEN try_shop_id          IS NOT NULL THEN 'Try::Shop'
          WHEN try_event_id         IS NOT NULL THEN 'Try::Event'
          WHEN try_walking_event_id IS NOT NULL THEN 'Try::WalkingEvent'
        END AS content_type,
        COALESCE(
          try_beauty_id,
          try_fitness_id,
          try_gourmet_id,
          try_life_hack_id,
          try_recipe_id,
          try_style_id,
          try_cool_id,
          try_shop_id,
          try_event_id,
          try_walking_event_id
        ) AS content_id
      FROM
        try_comments
      WHERE
        '{period_from}' <= updated_at AND updated_at <= '{period_to}'
       OR
        id IN (
          SELECT
            DISTINCT problem_reportable_id
          FROM
            problem_reports
          WHERE
            problem_reportable_type = 'Try::Comment'
           AND
            '{period_from}' <= updated_at AND updated_at <= '{period_to}'
        )
    ) t1_alias
    LEFT OUTER JOIN (
      SELECT
        problem_reportable_id, COUNT(problem_reportable_id) AS reported_count
      FROM
        problem_reports
      WHERE
        problem_reportable_type = 'Try::Comment'
      GROUP BY
        problem_reportable_type, problem_reportable_id
    ) t2_alias ON t1_alias.id = t2_alias.problem_reportable_id
    INNER JOIN users ON t1_alias.user_id = users.id
  ) t3_alias
'''

QUERY_FINC_USER_ID = '''
  (
    SELECT
      DATE_FORMAT(created_at, '%Y%m%d') AS created_on, id AS user_id, finc_user_id
    FROM
      users
    WHERE
      created_at BETWEEN '{period_from}' AND '{period_to}'
  ) t1_alias
'''

QUERY_CHAT_DIRECT_MESSAGES = '''
  (
    SELECT
      *
    FROM
      chat_direct_messages
    WHERE
      {period_from} < id AND id <= {period_to}
  ) t1_alias
'''

QUERY_GROUP_CHAT_USER_MESSAGES = '''
  (
    SELECT
      *
    FROM (
      SELECT
        *
      FROM
        group_chat_user_messages
      WHERE
        {period_from} < id AND id <= {period_to}
    ) t1_alias
    LEFT OUTER JOIN (
      SELECT
        sender_id
      FROM
        broadcast_post_group_senders
    ) t2_alias ON t1_alias.user_id = t2_alias.sender_id
    WHERE
      t2_alias.sender_id is NULL
      AND t1_alias.message != '部員専用掲示板'
      AND t1_alias.message != 'FiNCサポーター掲示板'
      AND t1_alias.message != '歩数ランキング'
      AND t1_alias.message != '質問BOX'
      AND t1_alias.message != 'ご質問BOX'
      AND t1_alias.message != 'ご質問・お問合せ'
      AND t1_alias.message != 'LIVE'
      AND t1_alias.message != 'チームを確認'
      AND t1_alias.message != 'ミッションを確認'
      AND t1_alias.message != 'ランキングを確認'
      AND t1_alias.message != 'ごほうびウォーカーへ'
      AND t1_alias.message != '特設ページへ'
      AND t1_alias.message != 'チャレンジ詳細'
      AND t1_alias.message != '~20代'
      AND t1_alias.message != '30代~40代'
      AND t1_alias.message != '50代~60代'
      AND t1_alias.message != '70代~'
      AND t1_alias.message != 'FiNCプレミアムの解約方法について'
      AND t1_alias.message != 'TO: チーム'
      AND t1_alias.message != '挨拶'
      AND t1_alias.message != '意気込み'
      AND t1_alias.message != 'v1.11'
  ) t3_alias
'''

QUERY_LETTER_BOX_MESSAGES = '''
  (
    SELECT
      t1_alias.id, t1_alias.uuid, text, t1_alias.created_at, t1_alias.updated_at, t1_alias.finc_user_id, t2_alias.uuid AS letter_uuid
    FROM (
      SELECT
        messages.id, messages.uuid, text, messages.created_at, messages.updated_at, finc_user_id
      FROM
        messages
      INNER JOIN texts ON messages.id = texts.message_id
      WHERE
        messages.type = 'Comment'
       AND
        '{period_from}' <= messages.updated_at AND messages.updated_at <= '{period_to}'
    ) t1_alias
    LEFT OUTER JOIN (
      SELECT
        ancestor_id,
        descendant_id,
        max(generations),
        uuid
      FROM
        message_hierarchies
      INNER JOIN messages ON messages.id = message_hierarchies.ancestor_id
      GROUP BY
        descendant_id
    ) t2_alias ON t1_alias.id = t2_alias.descendant_id
  ) t3_alias
'''
