#!/usr/bin/env bash
# Tags: no-parallel, no-random-merge-tree-settings

set -ue

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

## Create database and local tables
${CLICKHOUSE_CLIENT} -q "drop database if exists test_where_group_by_columns" "--query_id=03682_where_group_by_columns_000"
${CLICKHOUSE_CLIENT} -q "create database test_where_group_by_columns" "--query_id=03682_where_group_by_columns_001"
${CLICKHOUSE_CLIENT} -q "create table test_where_group_by_columns.left (id UInt64, name String, age UInt8, log_date Date, region String) engine = MergeTree order by id partition by log_date" "--query_id=03682_where_group_by_columns_002"
${CLICKHOUSE_CLIENT} -q "create table test_where_group_by_columns.right (id UInt64, region String, city String, age UInt8, log_date Date) engine = MergeTree order by id partition by log_date" "--query_id=03682_where_group_by_columns_003"

## Insert data
${CLICKHOUSE_CLIENT} -q "insert into test_where_group_by_columns.left values (1,'alice',20,'2024-05-27','north'), (2,'bob',25,'2024-05-28','south'), (3,'charlie',20,'2024-05-29','north')" "--query_id=03682_where_group_by_columns_004"
${CLICKHOUSE_CLIENT} -q "insert into test_where_group_by_columns.right values (1,'north','beijing',22,'2024-05-27'), (2,'south','shanghai',25,'2024-05-28')" "--query_id=03682_where_group_by_columns_005"

## Query local tables
${CLICKHOUSE_CLIENT} -q "select id,name,count() as cnt, sum(age) from test_where_group_by_columns.left where log_date <= '2024-05-28' and age >= 20 and age = 20 and id in (1,2,3) and name like 'a%' group by id,name order by id settings allow_experimental_analyzer = 0" "--query_id=03682_where_group_by_columns_006"
${CLICKHOUSE_CLIENT} -q "select id,name,count() as cnt, sum(age) from test_where_group_by_columns.left where log_date <= '2024-05-28' and age >= 20 and age = 20 and id in (1,2,3) and name like 'a%' group by id,name order by id settings allow_experimental_analyzer = 1" "--query_id=03682_where_group_by_columns_007"

${CLICKHOUSE_CLIENT} -q "select * from (select * from (select * from test_where_group_by_columns.left where log_date <= today()) where id >= 0) where name >= 'a' order by id settings allow_experimental_analyzer = 0" "--query_id=03682_where_group_by_columns_008"
${CLICKHOUSE_CLIENT} -q "select * from (select * from (select * from test_where_group_by_columns.left where log_date <= today()) where id >= 0) where name >= 'a' order by id settings allow_experimental_analyzer = 1" "--query_id=03682_where_group_by_columns_009"

${CLICKHOUSE_CLIENT} -q "select * from (select * from (select * from test_where_group_by_columns.left where log_date <= today() group by *) where id >= 0) where name >= 'a' order by id settings allow_experimental_analyzer = 0" "--query_id=03682_where_group_by_columns_010"
${CLICKHOUSE_CLIENT} -q "select * from (select * from (select * from test_where_group_by_columns.left where log_date <= today() group by *) where id >= 0) where name >= 'a' order by id settings allow_experimental_analyzer = 1" "--query_id=03682_where_group_by_columns_011"

${CLICKHOUSE_CLIENT} -q "select l.id from (select id,name from test_where_group_by_columns.left where log_date <= '2024-05-28' and age = 20 group by id,name) l join (select * from test_where_group_by_columns.right where age > 0) r on l.id = r.id order by l.id settings allow_experimental_analyzer = 0" "--query_id=03682_where_group_by_columns_012"
${CLICKHOUSE_CLIENT} -q "select l.id from (select id,name from test_where_group_by_columns.left where log_date <= '2024-05-28' and age = 20 group by id,name) l join (select * from test_where_group_by_columns.right where age > 0) r on l.id = r.id order by l.id settings allow_experimental_analyzer = 1" "--query_id=03682_where_group_by_columns_013"

${CLICKHOUSE_CLIENT} -q "select * from test_where_group_by_columns.left order by id settings allow_experimental_analyzer = 0" "--query_id=03682_where_group_by_columns_014"
${CLICKHOUSE_CLIENT} -q "select * from test_where_group_by_columns.left order by id settings allow_experimental_analyzer = 1" "--query_id=03682_where_group_by_columns_015"

${CLICKHOUSE_CLIENT} -q "select id as uid, name, age, count() from test_where_group_by_columns.left where uid > 0 group by uid, name, age order by uid settings allow_experimental_analyzer = 0" "--query_id=03682_where_group_by_columns_016"
${CLICKHOUSE_CLIENT} -q "select id as uid, name, age, count() from test_where_group_by_columns.left where uid > 0 group by uid, name, age order by uid settings allow_experimental_analyzer = 1" "--query_id=03682_where_group_by_columns_017"

${CLICKHOUSE_CLIENT} -q "select * from (select id, name, sum(age) as age, count() from test_where_group_by_columns.left group by id, name) where age > 10 order by id settings allow_experimental_analyzer = 0" "--query_id=03682_where_group_by_columns_018"
${CLICKHOUSE_CLIENT} -q "select * from (select id, name, sum(age) as age, count() from test_where_group_by_columns.left group by id, name) where age > 10 order by id settings allow_experimental_analyzer = 1" "--query_id=03682_where_group_by_columns_019"

${CLICKHOUSE_CLIENT} -q "select id, name, log_date, sum(age), count() as cnt from (select id, name, age, region, max(log_date) as log_date from test_where_group_by_columns.left group by id, name, age, region ) group by id, name, log_date settings allow_experimental_analyzer = 0" "--query_id=03682_where_group_by_columns_020"
${CLICKHOUSE_CLIENT} -q "select id, name, log_date, sum(age), count() as cnt from (select id, name, age, region, max(log_date) as log_date from test_where_group_by_columns.left group by id, name, age, region ) group by id, name, log_date settings allow_experimental_analyzer = 1" "--query_id=03682_where_group_by_columns_021"

## Create distributed tables
${CLICKHOUSE_CLIENT} -q "CREATE TABLE test_where_group_by_columns.left_all as test_where_group_by_columns.left engine = Distributed('test_shard_localhost', test_where_group_by_columns, left, rand());" "--query_id=03682_where_group_by_columns_022"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE test_where_group_by_columns.right_all as test_where_group_by_columns.right engine = Distributed('test_shard_localhost', test_where_group_by_columns, right, rand());" "--query_id=03682_where_group_by_columns_023"

## Query distributed tables
${CLICKHOUSE_CLIENT} -q "select id,name,count() as cnt, sum(age) from test_where_group_by_columns.left_all where log_date <= '2024-05-28' and age >= 20 and age = 20 and id in (1,2,3) and name like 'a%' group by id,name order by id settings allow_experimental_analyzer = 0, prefer_localhost_replica = 1" "--query_id=03682_where_group_by_columns_024"
${CLICKHOUSE_CLIENT} -q "select id,name,count() as cnt, sum(age) from test_where_group_by_columns.left_all where log_date <= '2024-05-28' and age >= 20 and age = 20 and id in (1,2,3) and name like 'a%' group by id,name order by id settings allow_experimental_analyzer = 1, prefer_localhost_replica = 1" "--query_id=03682_where_group_by_columns_025"
${CLICKHOUSE_CLIENT} -q "select id,name,count() as cnt, sum(age) from test_where_group_by_columns.left_all where log_date <= '2024-05-28' and age >= 20 and age = 20 and id in (1,2,3) and name like 'a%' group by id,name order by id settings allow_experimental_analyzer = 0, prefer_localhost_replica = 0" "--query_id=03682_where_group_by_columns_026"
${CLICKHOUSE_CLIENT} -q "select id,name,count() as cnt, sum(age) from test_where_group_by_columns.left_all where log_date <= '2024-05-28' and age >= 20 and age = 20 and id in (1,2,3) and name like 'a%' group by id,name order by id settings allow_experimental_analyzer = 1, prefer_localhost_replica = 0" "--query_id=03682_where_group_by_columns_027"


${CLICKHOUSE_CLIENT} -q "select * from (select * from (select * from test_where_group_by_columns.left_all where log_date <= today()) where id >= 0) where name >= 'a' order by id settings allow_experimental_analyzer = 0, prefer_localhost_replica = 1" "--query_id=03682_where_group_by_columns_028"
${CLICKHOUSE_CLIENT} -q "select * from (select * from (select * from test_where_group_by_columns.left_all where log_date <= today()) where id >= 0) where name >= 'a' order by id settings allow_experimental_analyzer = 1, prefer_localhost_replica = 1" "--query_id=03682_where_group_by_columns_029"
${CLICKHOUSE_CLIENT} -q "select * from (select * from (select * from test_where_group_by_columns.left_all where log_date <= today()) where id >= 0) where name >= 'a' order by id settings allow_experimental_analyzer = 0, prefer_localhost_replica = 0" "--query_id=03682_where_group_by_columns_030"
${CLICKHOUSE_CLIENT} -q "select * from (select * from (select * from test_where_group_by_columns.left_all where log_date <= today()) where id >= 0) where name >= 'a' order by id settings allow_experimental_analyzer = 1, prefer_localhost_replica = 0" "--query_id=03682_where_group_by_columns_031"

${CLICKHOUSE_CLIENT} -q "select * from (select * from (select * from test_where_group_by_columns.left_all where log_date <= today() group by *) where id >= 0) where name >= 'a' order by id settings allow_experimental_analyzer = 0, prefer_localhost_replica = 1" "--query_id=03682_where_group_by_columns_032"
${CLICKHOUSE_CLIENT} -q "select * from (select * from (select * from test_where_group_by_columns.left_all where log_date <= today() group by *) where id >= 0) where name >= 'a' order by id settings allow_experimental_analyzer = 1, prefer_localhost_replica = 1" "--query_id=03682_where_group_by_columns_033"
${CLICKHOUSE_CLIENT} -q "select * from (select * from (select * from test_where_group_by_columns.left_all where log_date <= today() group by *) where id >= 0) where name >= 'a' order by id settings allow_experimental_analyzer = 0, prefer_localhost_replica = 0" "--query_id=03682_where_group_by_columns_034"
${CLICKHOUSE_CLIENT} -q "select * from (select * from (select * from test_where_group_by_columns.left_all where log_date <= today() group by *) where id >= 0) where name >= 'a' order by id settings allow_experimental_analyzer = 1, prefer_localhost_replica = 0" "--query_id=03682_where_group_by_columns_035"

${CLICKHOUSE_CLIENT} -q "select l.id from (select id,name from test_where_group_by_columns.left_all where log_date <= '2024-05-28' and age = 20 group by id,name) l global join (select * from test_where_group_by_columns.right_all where age > 0) r on l.id = r.id order by l.id settings allow_experimental_analyzer = 0, prefer_localhost_replica = 1" "--query_id=03682_where_group_by_columns_036"
${CLICKHOUSE_CLIENT} -q "select l.id from (select id,name from test_where_group_by_columns.left_all where log_date <= '2024-05-28' and age = 20 group by id,name) l global join (select * from test_where_group_by_columns.right_all where age > 0) r on l.id = r.id order by l.id settings allow_experimental_analyzer = 1, prefer_localhost_replica = 1" "--query_id=03682_where_group_by_columns_037"
${CLICKHOUSE_CLIENT} -q "select l.id from (select id,name from test_where_group_by_columns.left_all where log_date <= '2024-05-28' and age = 20 group by id,name) l global join (select * from test_where_group_by_columns.right_all where age > 0) r on l.id = r.id order by l.id settings allow_experimental_analyzer = 0, prefer_localhost_replica = 0" "--query_id=03682_where_group_by_columns_038"
${CLICKHOUSE_CLIENT} -q "select l.id from (select id,name from test_where_group_by_columns.left_all where log_date <= '2024-05-28' and age = 20 group by id,name) l global join (select * from test_where_group_by_columns.right_all where age > 0) r on l.id = r.id order by l.id settings allow_experimental_analyzer = 1, prefer_localhost_replica = 0" "--query_id=03682_where_group_by_columns_039"

${CLICKHOUSE_CLIENT} -q "select * from test_where_group_by_columns.left_all order by id settings allow_experimental_analyzer = 0, prefer_localhost_replica = 1" "--query_id=03682_where_group_by_columns_040"
${CLICKHOUSE_CLIENT} -q "select * from test_where_group_by_columns.left_all order by id settings allow_experimental_analyzer = 1, prefer_localhost_replica = 1" "--query_id=03682_where_group_by_columns_041"
${CLICKHOUSE_CLIENT} -q "select * from test_where_group_by_columns.left_all order by id settings allow_experimental_analyzer = 0, prefer_localhost_replica = 0" "--query_id=03682_where_group_by_columns_042"
${CLICKHOUSE_CLIENT} -q "select * from test_where_group_by_columns.left_all order by id settings allow_experimental_analyzer = 1, prefer_localhost_replica = 0" "--query_id=03682_where_group_by_columns_043"

${CLICKHOUSE_CLIENT} -q "select id as uid, name, age, count() from test_where_group_by_columns.left_all where uid > 0 group by uid, name, age order by uid settings allow_experimental_analyzer = 0, prefer_localhost_replica = 1" "--query_id=03682_where_group_by_columns_044"
${CLICKHOUSE_CLIENT} -q "select id as uid, name, age, count() from test_where_group_by_columns.left_all where uid > 0 group by uid, name, age order by uid settings allow_experimental_analyzer = 1, prefer_localhost_replica = 1" "--query_id=03682_where_group_by_columns_045"
${CLICKHOUSE_CLIENT} -q "select id as uid, name, age, count() from test_where_group_by_columns.left_all where uid > 0 group by uid, name, age order by uid settings allow_experimental_analyzer = 0, prefer_localhost_replica = 0" "--query_id=03682_where_group_by_columns_046"
${CLICKHOUSE_CLIENT} -q "select id as uid, name, age, count() from test_where_group_by_columns.left_all where uid > 0 group by uid, name, age order by uid settings allow_experimental_analyzer = 1, prefer_localhost_replica = 0" "--query_id=03682_where_group_by_columns_047"

${CLICKHOUSE_CLIENT} -q "select * from (select id, name, sum(age) as age, count() from test_where_group_by_columns.left_all group by id, name) where age > 10 order by id settings allow_experimental_analyzer = 0, prefer_localhost_replica = 1" "--query_id=03682_where_group_by_columns_048"
${CLICKHOUSE_CLIENT} -q "select * from (select id, name, sum(age) as age, count() from test_where_group_by_columns.left_all group by id, name) where age > 10 order by id settings allow_experimental_analyzer = 1, prefer_localhost_replica = 1" "--query_id=03682_where_group_by_columns_049"
${CLICKHOUSE_CLIENT} -q "select * from (select id, name, sum(age) as age, count() from test_where_group_by_columns.left_all group by id, name) where age > 10 order by id settings allow_experimental_analyzer = 0, prefer_localhost_replica = 0" "--query_id=03682_where_group_by_columns_050"
${CLICKHOUSE_CLIENT} -q "select * from (select id, name, sum(age) as age, count() from test_where_group_by_columns.left_all group by id, name) where age > 10 order by id settings allow_experimental_analyzer = 1, prefer_localhost_replica = 0" "--query_id=03682_where_group_by_columns_051"

${CLICKHOUSE_CLIENT} -q "select id, name, log_date, sum(age), count() as cnt from (select id, name, age, region, max(log_date) as log_date from test_where_group_by_columns.left_all group by id, name, age, region ) group by id, name, log_date settings allow_experimental_analyzer = 0, prefer_localhost_replica = 1" "--query_id=03682_where_group_by_columns_052"
${CLICKHOUSE_CLIENT} -q "select id, name, log_date, sum(age), count() as cnt from (select id, name, age, region, max(log_date) as log_date from test_where_group_by_columns.left_all group by id, name, age, region ) group by id, name, log_date settings allow_experimental_analyzer = 1, prefer_localhost_replica = 1" "--query_id=03682_where_group_by_columns_053"
${CLICKHOUSE_CLIENT} -q "select id, name, log_date, sum(age), count() as cnt from (select id, name, age, region, max(log_date) as log_date from test_where_group_by_columns.left_all group by id, name, age, region ) group by id, name, log_date settings allow_experimental_analyzer = 0, prefer_localhost_replica = 0" "--query_id=03682_where_group_by_columns_054"
${CLICKHOUSE_CLIENT} -q "select id, name, log_date, sum(age), count() as cnt from (select id, name, age, region, max(log_date) as log_date from test_where_group_by_columns.left_all group by id, name, age, region ) group by id, name, log_date settings allow_experimental_analyzer = 1, prefer_localhost_replica = 0" "--query_id=03682_where_group_by_columns_055"

# Cleanup
${CLICKHOUSE_CLIENT} -q "drop database if exists test_where_group_by_columns" "--query_id=03682_where_group_by_columns_056"

# Flush logs and query the results
${CLICKHOUSE_CLIENT} -q "system flush logs"
${CLICKHOUSE_CLIENT} -q "select initial_query_id, where_columns, group_by_columns from system.query_log where type = 'QueryFinish' and initial_query_id like '03682_where_group_by_columns%' and query_id not like '%system%' order by initial_query_id, is_initial_query, tables"
