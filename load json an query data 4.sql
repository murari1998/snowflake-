create or replace database our_first_db;

create or replace schema manage_db.external_stages;

// first step : load raw data

create or replace stage manage_db.external_stages.jsonstage
url='s3://myjson123'
credentials=(
aws_key_id=''
aws_secret_key='');


list @manage_db.external_stages.jsonstage;


-- creating schema inside manage_db 
create or replace schema manage_db.file_formats;


// create a file format for json file
-- stage is jsonstage,  fileformat jsonformat
create or replace file format manage_db.file_formats.jsonformat
 type=json;


create or replace table our_first_db.public.json_raw(
 raw_file variant);

-- stage is jsonstage fileformat jsonformat table json_raw
 copy into our_first_db.public.json_raw
  from @manage_db.external_stages.jsonstage
  file_format=manage_db.file_formats.jsonformat
  files=('HR_data (2).json');

  select * from our_first_db.public.json_raw;

select raw_file, raw_file:city, raw_file:first_name from our_first_db.public.json_raw;


select raw_file, raw_file:gender from our_first_db.public.json_raw;

select raw_file, raw_file:city::string from our_first_db.public.json_raw;

select raw_file, raw_file:city from our_first_db.public.json_raw;

select raw_file:city::string, $1:city from our_first_db.public.json_raw;


select  $1:first_name from our_first_db.public.json_raw;

select raw_file:first_name::string as first_name from our_first_db.public.json_raw;

select raw_file:id::int as id from our_first_db.public.json_raw;

select raw_file:id::int as id,
raw_file:first_name::string as first_name,
raw_file:last_name::string as last_name,
raw_file:gender::string as gender
from our_first_db.public.json_raw;


select raw_file, raw_file:prev_company as prev_company
from our_first_db.public.json_raw;

select raw_file:prev_company[0]::string as prev_company
from our_first_db.public.json_raw;


select 
array_size(raw_file:prev_company) as prev_company
from our_first_db.public.json_raw;


// Handling nested data
   


// Handling arrays

SELECT
    raw_file, RAW_FILE:spoken_languages[0]:language::string as prev_company
FROM OUR_FIRST_DB.PUBLIC.JSON_RAW;

-- to extract data from prev_company
SELECT
    RAW_FILE:prev_company[0]::STRING as prev_company
FROM OUR_FIRST_DB.PUBLIC.JSON_RAW;


SELECT
    ARRAY_SIZE(RAW_FILE:prev_company) as prev_company
FROM OUR_FIRST_DB.PUBLIC.JSON_RAW;




SELECT
    RAW_FILE:id::int as id,  
    RAW_FILE:first_name::STRING as first_name,
    RAW_FILE:prev_company[0]::STRING as prev_company
FROM OUR_FIRST_DB.PUBLIC.JSON_RAW
UNION ALL
SELECT
    RAW_FILE:id::int as id,  
    RAW_FILE:first_name::STRING as first_name,
    RAW_FILE:prev_company[1]::STRING as prev_company
FROM OUR_FIRST_DB.PUBLIC.JSON_RAW
ORDER BY id;

-- -------------------#########--------------#######



SELECT
    RAW_FILE:spoken_languages as spoken_languages
FROM OUR_FIRST_DB.PUBLIC.JSON_RAW;

SELECT * FROM OUR_FIRST_DB.PUBLIC.JSON_RAW;


SELECT
     array_size(RAW_FILE:spoken_languages) as spoken_languages
FROM OUR_FIRST_DB.PUBLIC.JSON_RAW;


SELECT
     RAW_FILE:first_name::STRING as first_name,
     array_size(RAW_FILE:spoken_languages) as spoken_languages
FROM OUR_FIRST_DB.PUBLIC.JSON_RAW order by spoken_languages desc;



SELECT
    RAW_FILE:spoken_languages[0] as First_language
FROM OUR_FIRST_DB.PUBLIC.JSON_RAW;


SELECT
    RAW_FILE:first_name::STRING as first_name,
    RAW_FILE:spoken_languages[0] as First_language
FROM OUR_FIRST_DB.PUBLIC.JSON_RAW;


SELECT
    RAW_FILE:first_name::STRING as First_name,
    RAW_FILE:spoken_languages[0].language::STRING as First_language,
    RAW_FILE:spoken_languages[0].level::STRING as Level_spoken
FROM OUR_FIRST_DB.PUBLIC.JSON_RAW;




SELECT
    RAW_FILE:id::int as id,
    RAW_FILE:first_name::STRING as First_name,
    RAW_FILE:spoken_languages[0].language::STRING as First_language,
    RAW_FILE:spoken_languages[0].level::STRING as Level_spoken
FROM OUR_FIRST_DB.PUBLIC.JSON_RAW
UNION ALL
SELECT
    RAW_FILE:id::int as id,
    RAW_FILE:first_name::STRING as First_name,
    RAW_FILE:spoken_languages[1].language::STRING as First_language,
    RAW_FILE:spoken_languages[1].level::STRING as Level_spoken
FROM OUR_FIRST_DB.PUBLIC.JSON_RAW
UNION ALL
SELECT
    RAW_FILE:id::int as id,
    RAW_FILE:first_name::STRING as First_name,
    RAW_FILE:spoken_languages[2].language::STRING as First_language,
    RAW_FILE:spoken_languages[2].level::STRING as Level_spoken
FROM OUR_FIRST_DB.PUBLIC.JSON_RAW
ORDER BY ID;




select
      RAW_FILE:first_name::STRING as First_name,
    f.value:language::STRING as First_language,
   f.value:level::STRING as Level_spoken
from OUR_FIRST_DB.PUBLIC.JSON_RAW, table(flatten(RAW_FILE:spoken_languages)) f;











--  ---------------------- ### ---------------###  (5)
// Option 1: CREATE TABLE AS

CREATE OR REPLACE TABLE Languages AS
select
      RAW_FILE:first_name::STRING as First_name,
    f.value:language::STRING as First_language,
   f.value:level::STRING as Level_spoken
from OUR_FIRST_DB.PUBLIC.JSON_RAW, table(flatten(RAW_FILE:spoken_languages)) f;

SELECT * FROM Languages;

truncate table languages;

// Option 2: INSERT INTO

INSERT INTO Languages
select
      RAW_FILE:first_name::STRING as First_name,
    f.value:language::STRING as First_language,
   f.value:level::STRING as Level_spoken
from OUR_FIRST_DB.PUBLIC.JSON_RAW, table(flatten(RAW_FILE:spoken_languages)) f;


SELECT * FROM Languages;








  
  

 



