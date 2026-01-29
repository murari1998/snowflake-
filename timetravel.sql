-- timetravel-- 

use orderdb;

create or replace table test(id int,ename varchar(20));
insert into test values(10,'abc'),(11,'def');

select * from test;

update test set ename='murari';

select * from test;


select * from test at (offset => -60*1);

select * from test;


-- ---------------
-- using query id
-- ------------------

select * from test before(statement => '01c1e5a0-3202-40b4-0013-4e9e0001d02a');


-- -----------
-- usnig timestamp
-- --------------------


