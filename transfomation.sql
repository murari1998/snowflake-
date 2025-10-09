use managedb;


// example 3 -table

create or replace stage managedb.external_stages.aws_stage
url=''
credentials=(
aws_key_id=''
aws_secret_key=''
);

list @aws_stage;

// creating a table to have data from the stages

create or replace table managedb.public.ratingtest(userid int,movieid int,rating int);

desc table ratingtest;

select * from ratingtest;

// example 4 -using subset of columns
// accessing 3 columns from stage   s.$1 is first columns
copy into managedb.public.ratingtest
from(select
     s.$1,
     s.$2,
     s.$3
     from @managedb.external_stages.aws_stage s)
     file_format=(type=csv field_delimiter=',' skip_header=1)
     files=('ratings.csv');

     select * from ratingtest;


     // example 5
// add a new columns foe easy to query but not data column
   create or replace table managedb.public.ratingtest2(id int autoincrement start 1 increment 1,userid int,movieid int,rating int);  

   copy into managedb.public.ratingtest2(userid,movieid,rating)
from(select
     s.$1,s.$2,s.$3
     from @managedb.external_stages.aws_stage s)
     file_format=(type=csv field_delimiter=',' skip_header=1)
     files=('ratings.csv');

     select * from ratingtest2;





//-----------------

     create or replace table
managedb.public.ratingtest3( timestamp int );


COPY INTO managedb.public.ratingtest3
    FROM (select  s.$4
          from @MANAGEDB.external_stages.aws_stage s)
    file_format= (type = csv field_delimiter=',' skip_header=1)
        files=('ratings.csv');







create or replace table
managedb.public.ratingtest_timestamp( timestamp int, time_year int, time_month int );

insert into ratingtest_timestamp  
select $1, year(to_timestamp($1) ), month(to_timestamp($1) ) from ratingtest3;


select * from ratingtest3;