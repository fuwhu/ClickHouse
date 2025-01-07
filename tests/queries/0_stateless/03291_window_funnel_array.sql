drop table if exists funnel_test;

create table funnel_test (timestamps Array(UInt32), event UInt32) engine=Memory;
insert into funnel_test values ([0],1000),([1],1001),([2],1002),([3],1003),([4],1004),([5],1005),([6],1006),([7],1007),([8],1008);

select 1 = windowFunnel(10000)(timestamps, event = 1000) from funnel_test;
select 2 = windowFunnel(10000)(timestamps, event = 1000, event = 1001) from funnel_test;
select 3 = windowFunnel(10000)(timestamps, event = 1000, event = 1001, event = 1002) from funnel_test;
select 4 = windowFunnel(10000)(timestamps, event = 1000, event = 1001, event = 1002, event = 1008) from funnel_test;

select 1 = windowFunnel(1)(timestamps, event = 1000) from funnel_test;
select 3 = windowFunnel(2)(timestamps, event = 1003, event = 1004, event = 1005, event = 1006, event = 1007) from funnel_test;
select 4 = windowFunnel(3)(timestamps, event = 1003, event = 1004, event = 1005, event = 1006, event = 1007) from funnel_test;
select 5 = windowFunnel(4)(timestamps, event = 1003, event = 1004, event = 1005, event = 1006, event = 1007) from funnel_test;

drop table if exists funnel_test2;
create table funnel_test2 (timestamps Array(DateTime), event UInt32) engine=Memory;
insert into funnel_test2 values  (['2018-01-01 01:01:01'],1001),(['2018-01-01 01:01:02'],1002),(['2018-01-01 01:01:03'],1003),(['2018-01-01 01:01:04'],1004),(['2018-01-01 01:01:05'],1005),(['2018-01-01 01:01:06'],1006),(['2018-01-01 01:01:07'],1007),(['2018-01-01 01:01:08'],1008);

select 5 = windowFunnel(4)(timestamps, event = 1003, event = 1004, event = 1005, event = 1006, event = 1007) from funnel_test2;
select 2 = windowFunnel(10000)(timestamps, event = 1001, event = 1008) from funnel_test2;
select 1 = windowFunnel(10000)(timestamps, event = 1008, event = 1001) from funnel_test2;
select 5 = windowFunnel(4)(timestamps, event = 1003, event = 1004, event = 1005, event = 1006, event = 1007) from funnel_test2;
select 4 = windowFunnel(4)(timestamps, event <= 1007, event >= 1002, event <= 1006, event >= 1004) from funnel_test2;

drop table if exists funnel_test_strict;
create table funnel_test_strict (timestamps Array(UInt32), event UInt32) engine=Memory;
insert into funnel_test_strict values ([00],1000),([10],1001),([20],1002),([30],1003),([40],1004),([50,51],1005),([60],1006),([70],1007),([80],1008);

select 6 = windowFunnel(10000, 'strict_deduplication')(timestamps, event = 1000, event = 1001, event = 1002, event = 1003, event = 1004, event = 1005, event = 1006) from funnel_test_strict;
select 7 = windowFunnel(10000)(timestamps, event = 1000, event = 1001, event = 1002, event = 1003, event = 1004, event = 1005, event = 1006) from funnel_test_strict;

drop table if exists funnel_test_strict_increase;
create table funnel_test_strict_increase (timestamps Array(UInt32), event UInt32) engine=Memory;
insert into funnel_test_strict_increase values ([0],1000),([1],1001),([1],1002),([1],1003),([2],1004);

select 5 = windowFunnel(10000)(timestamps, event = 1000, event = 1001, event = 1002, event = 1003, event = 1004) from funnel_test_strict_increase;
select 2 = windowFunnel(10000, 'strict_increase')(timestamps, event = 1000, event = 1001, event = 1002, event = 1003, event = 1004) from funnel_test_strict_increase;
select 3 = windowFunnel(10000)(timestamps, event = 1004, event = 1004, event = 1004) from funnel_test_strict_increase;
select 1 = windowFunnel(10000, 'strict_increase')(timestamps, event = 1004, event = 1004, event = 1004) from funnel_test_strict_increase;

drop table funnel_test;
drop table funnel_test2;
drop table funnel_test_strict;
drop table funnel_test_strict_increase;
