/*This script contains sql queries to setup snowpipe in
 Snowflake and connecting S3 bucket with it*/

--create warehouse 
use role sysadmin;
create or replace warehouse twitter_wh
with warehouse_size ='XSMALL'
auto_suspend = 120
auto_resume = true;

--create database and table
use warehouse twitter_wh;
create database if not exists twitter_stream;

use twitter_stream.public;

create or replace table twitter_stream.public.tweets(
    tweet variant
);

--create external stage to S3 bucket
create or replace stage twitter_stream.public.twitter_stage
url = 's3://realtimestreamtweets/'
credentials = (aws_key_id = '<key>', aws_secret_key = '<secret key>');

--create pipe from external stage
create or replace pipe twitter_stream.public.snowpipe 
auto_ingest = true as 
copy into twitter_stream.public.tweets
from @twitter_stream.public.twitter_stage
file_format = (type = 'JSON');

--get notification channel to configure s3 bucket to the add data into SQS queue
show pipes;

--check if data got loaded from s3
select count(*) from twitter_stream.public.tweets;