use test;
create table testtable1 if not exists (id int, name varchar(20));
delete from testtable1 where true;
insert into testtable1 (id,name) values 
(1,'name1'), 
(2, 'name2');

create table testtable2 if not exists (id int, name varchar(20));
delete from testtable2 where true;
insert into testtable2 (id,name) values 
(1,'name1'), 
(2, 'name2');

create table temptable1 if not exists (id int, name varchar(20));
