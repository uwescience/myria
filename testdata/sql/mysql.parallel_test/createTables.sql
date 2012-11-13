use test;
create table if not exists testtable1 (id int, name varchar(20));
delete from testtable1 where true;
insert into testtable1 (id,name) values 
(1,'name1'), 
(2, 'name2');

create table if not exists testtable2 (id int, name varchar(20));
delete from testtable2 where true;
insert into testtable2 (id,name) values 
(1,'name1'), 
(2, 'name2');

create table if not exists temptable1 (id int, name varchar(20));
