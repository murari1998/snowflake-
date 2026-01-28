
create database stream_database;
use stream_database;
create or replace table sales_raw_staging(id int,product varchar,price varchar,amount varchar,
store_id varchar);


insert into sales_raw_staging(id,product,price,amount,store_id) values
(1,'banana',1.09,1,1),
(2,'apple',2.09,1,2),
(3,'cherry',3.09,1,3),
(4,'orangejuice',4.08,1,4),
(5,'cerelas',5.07,1,5);

create or replace table store_table (store_id varchar,location varchar,employees varchar);

select * from store_table;

insert into store_table values(1,'mumbai','dheera'),
(2,'delhi','dheeru');

  create or replace  stream store_stream on table sales_raw_staging
  APPEND_ONLY=TRUE;

select * from store_stream;
select * from sales_raw_staging;

insert into sales_raw_staging values(6,'choclate shake',6.09,1,6),
(7,'milk shake',7.09,1,7);






create or replace table  sales_final_table
(id int,product varchar,price varchar,amount varchar,
store_id varchar,location varchar,employees varchar);

insert into sales_final_table select
srs.id,srs.product,srs.price,srs.amount,
st.store_id,st.location,st.employees
from sales_raw_staging as srs join store_table as st on srs.store_id=st.store_id;

select * from sales_final_table;

update sales_raw_staging
set product='guvava' where id=1;

select * from sales_raw_staging;
select * from sales_final_table;
SELECT * FROM STORE_STREAM;

MERGE INTO  sales_final_table  AS sft
USING
store_stream AS ss
on sft.id=ss.id
WHEN MATCHED
AND ss.METADATA$ACTION='DELETE'
THEN DELETE;


MERGE INTO  sales_final_table  AS sft
USING
store_stream AS ss
on sft.id=ss.id
WHEN MATCHED
AND ss.METADATA$ACTION='INSERT'
AND ss.METADATA$ISUPDATE=TRUE
THEN UPDATE SET
sft.PRODUCT=ss.PRODUCT,
sft.PRICE=ss.PRICE,
sft.AMOUNT=ss.AMOUNT;