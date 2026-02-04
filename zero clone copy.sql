create database sai2;
create database db_clone clone sai2;

use sai2;

create or replace table msai(id int,name varchar(20),city varchar(30));
select * from msai;
insert into msai values(1,'jassi','vapi'),
(2,'parmod','nwhl');

create or replace table msai2 clone msai;

select * from msai2;

use sai2;

create or replace schema jassi;

create or replace schema jassi_clone clone jassi;

use schema jassi;

create or replace table jass (id int,name varchar);

desc schema jassi_clone;