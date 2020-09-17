DROP TABLE summary_tables.secondly_active_users;

CREATE TABLE summary_tables.secondly_active_users(
  finc_user_id BIGINT NOT NULL,
  active_at TIMESTAMP
)
SORTKEY(active_at);

GRANT SELECT ON TABLE summary_tables.secondly_active_users TO analytics_dms;

DROP TABLE summary_tables.secondly_active_users_test;

CREATE TABLE summary_tables.secondly_active_users_test(
  finc_user_id BIGINT NOT NULL,
  active_at TIMESTAMP
)
SORTKEY(active_at);

GRANT SELECT ON TABLE summary_tables.secondly_active_users_test TO analytics_dms;


create external schema external_summary_tables
from data catalog 
database 'analytics' 
iam_role 'arn:aws:iam::759549166074:role/finc-redshift-spectrum-role'

CREATE EXTERNAL TABLE external_summary_tables.secondly_active_users(
  finc_user_id BIGINT,
  start_time TIMESTAMP,
  end_time TIMESTAMP,
  tick TIMESTAMP
)
partitioned by (accessdate char(10))
stored as parquet
location 's3://data-lake-for-analytics/external_summary_tables/secondly_active_users/';

GRANT USAGE ON SCHEMA external_summary_tables TO analytics_dms;
GRANT SELECT ON ALL TABLES IN SCHEMA external_summary_tables TO analytics_dms;


alter table external_summary_tables.secondly_active_users add
partition(accessdate='2020-04-19')
location 's3://data-lake-for-analytics/external_summary_tables/secondly_active_users/accessdate=2020-04-19/'

COPY summary_tables.secondly_active_users
FROM 's3://data-lake-for-analytics/external_summary_tables/secondly_active_users/accessdate=2020-04-19/'
IAM_ROLE 'arn:aws:iam::759549166074:role/finc-redshift-spectrum-role'
FORMAT AS PARQUET;


CREATE TABLE summary_tables.secondly_active_users_tmp(
  end_time TIMESTAMP,
  finc_user_id BIGINT NOT NULL,
  start_time TIMESTAMP,
  tick TIMESTAMP
)
SORTKEY(tick);