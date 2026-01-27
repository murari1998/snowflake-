// Create table first

use orderdb;

CREATE OR REPLACE STAGE murari_bucket_stage
  URL = 's3://demosnoepipe'
  CREDENTIALS = (AWS_KEY_ID = 'A'
    AWS_SECRET_KEY = 'hnNH' );

list @murari_bucket_stage;


create or replace table employee(eid int, ename varchar(20), age int);
select * from employee;

-- copy command => load data from the external location to snowflake table
copy into employee
from @murari_bucket_stage
files=('data_fri.csv')
file_format=( field_delimiter='-', skip_header=1);


// Define pipe
CREATE OR REPLACE pipe employee_pipe
auto_ingest = TRUE
AS
copy into employee
from @murari_bucket_stage
file_format=( field_delimiter='-', skip_header=1) ;

// Describe pipe
DESC pipe employee_pipe;
   
SELECT * FROM employee ;







----------------------------------------------------






 Post by REGex Software
REGex Software
Created 20 Jan20 Jan

-- 20th Jan - snowflake files

create database if not exists orderdb;

create schema order_ext_stage;

create stage order_ext_stage_orderSample
  URL = 's3://tushar-bucket-regex'
  CREDENTIALS = (AWS_KEY_ID = 'A'
    AWS_SECRET_KEY = 'V1F' );


list @order_ext_stage_orderSample;
describe stage order_ext_stage_orderSample;

create or replace table orderTable( col variant);
select * from orderTable;

-- orders_sample (1).json
copy into orderTable
from @order_ext_stage_orderSample
file_format=(type=JSON)
files=('orders_sample (1).json');

select  f.value:items[0]:attributes ,substr( f.value:customer:customer_id::string, -2) as cid from orderTable,
table(flatten(col)) f;



select * from customer_dim;