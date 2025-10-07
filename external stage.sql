create or replace database manage_db;
create or replace schema external_schema;

use database manage_db;
use schema external_schema;

create or replace table movies(
movieid int,
title varchar(200),
genres varchar(100));

create or replace file format my_csv
type='csv'
skip_header=1;

create or replace stage my_s3_stage
url=''
credentials=(
aws_key_id=''
aws_secret_key=''

)
file_format=my_csv;
 
list @my_s3_stage;

copy into movies
from @my_s3_stage
file_format=(format_name=my_csv);

select * from movies;

drop database manage_db;