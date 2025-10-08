// createing database

create or replace database managedb;

use managedb;

// creating schema inside data warehouse

create or replace schema external_stages;

// createing external stages

create or replace stage managedb.external_stages.aws_stage
url=''
credentials=(
aws_key_id='asdfghjkl'
aws_secret_key='wedfgbnmkloiuygfcx'
);

list @aws_stage;

// creating a table to have data from the stages

create or replace table managedb.public.ratingtest(userid int,movieid int,rating int,timestamp int);

desc table ratingtest;

// load data form the datalake into the table

copy into ratingtest
from @aws_stage/ratings.csv
file_format=(type='csv',skip_header=1);

select * from ratingtest;

