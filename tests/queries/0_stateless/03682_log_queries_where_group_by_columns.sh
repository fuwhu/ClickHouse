#!/usr/bin/env bash
# Tags: no-parallel, no-random-merge-tree-settings

set -ue

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Create test database and tables
${CLICKHOUSE_CLIENT} -q "drop database if exists test_where_group_by_columns" "--query_id=03682_where_group_by_columns_000"
${CLICKHOUSE_CLIENT} -q "create database test_where_group_by_columns" "--query_id=03682_where_group_by_columns_001"
${CLICKHOUSE_CLIENT} -q "create table test_where_group_by_columns.left (id UInt64, name String, age UInt8, log_date Date, region String) engine = MergeTree order by id partition by log_date" "--query_id=03682_where_group_by_columns_002"
${CLICKHOUSE_CLIENT} -q "create table test_where_group_by_columns.right (id UInt64, region String, city String, age UInt8, log_date Date) engine = MergeTree order by id partition by log_date" "--query_id=03682_where_group_by_columns_003"

# Insert test data
${CLICKHOUSE_CLIENT} -q "insert into test_where_group_by_columns.left values (1,'alice',20,'2024-05-27','north'), (2,'bob',25,'2024-05-28','south'), (3,'charlie',20,'2024-05-29','north')" "--query_id=03682_where_group_by_columns_004"
${CLICKHOUSE_CLIENT} -q "insert into test_where_group_by_columns.right values (1,'north','beijing',22,'2024-05-27'), (2,'south','shanghai',25,'2024-05-28')" "--query_id=03682_where_group_by_columns_005"

# Test 1: Query with WHERE (equality, range, other) and GROUP BY
${CLICKHOUSE_CLIENT} -q "select id,name,count() as cnt, sum(age) from test_where_group_by_columns.left where log_date <= '2024-05-28' and age >= 20 and age = 20 and id in (1,2,3) and name like 'a%' group by id,name order by id settings allow_experimental_analyzer = 0" "--query_id=03682_where_group_by_columns_006"
${CLICKHOUSE_CLIENT} -q "select id,name,count() as cnt, sum(age) from test_where_group_by_columns.left where log_date <= '2024-05-28' and age >= 20 and age = 20 and id in (1,2,3) and name like 'a%' group by id,name order by id settings allow_experimental_analyzer = 1" "--query_id=03682_where_group_by_columns_007"

# Test 2: Nested subquery with multiple WHERE conditions (range only)
${CLICKHOUSE_CLIENT} -q "select * from (select * from (select * from test_where_group_by_columns.left where log_date <= today()) where id >= 0) where name >= 'a' order by id settings allow_experimental_analyzer = 0" "--query_id=03682_where_group_by_columns_008"
${CLICKHOUSE_CLIENT} -q "select * from (select * from (select * from test_where_group_by_columns.left where log_date <= today()) where id >= 0) where name >= 'a' order by id settings allow_experimental_analyzer = 1" "--query_id=03682_where_group_by_columns_009"

# Test 3: Nested subquery with WHERE and GROUP BY *
${CLICKHOUSE_CLIENT} -q "select * from (select * from (select * from test_where_group_by_columns.left where log_date <= today() group by *) where id >= 0) where name >= 'a' order by id settings allow_experimental_analyzer = 0" "--query_id=03682_where_group_by_columns_010"
${CLICKHOUSE_CLIENT} -q "select * from (select * from (select * from test_where_group_by_columns.left where log_date <= today() group by *) where id >= 0) where name >= 'a' order by id settings allow_experimental_analyzer = 1" "--query_id=03682_where_group_by_columns_011"

# Test 4: JOIN query with WHERE and GROUP BY
${CLICKHOUSE_CLIENT} -q "select l.id from (select id,name from test_where_group_by_columns.left where log_date <= '2024-05-28' and age = 20 group by id,name) l join (select * from test_where_group_by_columns.right where age > 0) r on l.id = r.id order by l.id settings allow_experimental_analyzer = 0" "--query_id=03682_where_group_by_columns_012"
${CLICKHOUSE_CLIENT} -q "select l.id from (select id,name from test_where_group_by_columns.left where log_date <= '2024-05-28' and age = 20 group by id,name) l join (select * from test_where_group_by_columns.right where age > 0) r on l.id = r.id order by l.id settings allow_experimental_analyzer = 1" "--query_id=03682_where_group_by_columns_013"

# Test 5: Simple SELECT * (no WHERE, no GROUP BY)
${CLICKHOUSE_CLIENT} -q "select * from test_where_group_by_columns.left order by id settings allow_experimental_analyzer = 0" "--query_id=03682_where_group_by_columns_014"
${CLICKHOUSE_CLIENT} -q "select * from test_where_group_by_columns.left order by id settings allow_experimental_analyzer = 1" "--query_id=03682_where_group_by_columns_015"

# Create distributed tables
${CLICKHOUSE_CLIENT} -q "CREATE TABLE test_where_group_by_columns.left_all as test_where_group_by_columns.left engine = Distributed('test_shard_localhost', test_where_group_by_columns, left, rand());" "--query_id=03682_where_group_by_columns_016"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE test_where_group_by_columns.right_all as test_where_group_by_columns.right engine = Distributed('test_shard_localhost', test_where_group_by_columns, right, rand());" "--query_id=03682_where_group_by_columns_017"

# Test 6: Query with WHERE (equality, range, other) and GROUP BY
${CLICKHOUSE_CLIENT} -q "select id,name,count() as cnt, sum(age) from test_where_group_by_columns.left_all where log_date <= '2024-05-28' and age >= 20 and age = 20 and id in (1,2,3) and name like 'a%' group by id,name order by id settings allow_experimental_analyzer = 0, prefer_localhost_replica = 0" "--query_id=03682_where_group_by_columns_018"
${CLICKHOUSE_CLIENT} -q "select id,name,count() as cnt, sum(age) from test_where_group_by_columns.left_all where log_date <= '2024-05-28' and age >= 20 and age = 20 and id in (1,2,3) and name like 'a%' group by id,name order by id settings allow_experimental_analyzer = 1, prefer_localhost_replica = 0" "--query_id=03682_where_group_by_columns_019"

# Test 7: Nested subquery with multiple WHERE conditions (range only)
${CLICKHOUSE_CLIENT} -q "select * from (select * from (select * from test_where_group_by_columns.left_all where log_date <= today()) where id >= 0) where name >= 'a' order by id settings allow_experimental_analyzer = 0, prefer_localhost_replica = 0" "--query_id=03682_where_group_by_columns_020"
${CLICKHOUSE_CLIENT} -q "select * from (select * from (select * from test_where_group_by_columns.left_all where log_date <= today()) where id >= 0) where name >= 'a' order by id settings allow_experimental_analyzer = 1, prefer_localhost_replica = 0" "--query_id=03682_where_group_by_columns_021"

# Test 8: Nested subquery with WHERE and GROUP BY *
${CLICKHOUSE_CLIENT} -q "select * from (select * from (select * from test_where_group_by_columns.left_all where log_date <= today() group by *) where id >= 0) where name >= 'a' order by id settings allow_experimental_analyzer = 0, prefer_localhost_replica = 0" "--query_id=03682_where_group_by_columns_022"
${CLICKHOUSE_CLIENT} -q "select * from (select * from (select * from test_where_group_by_columns.left_all where log_date <= today() group by *) where id >= 0) where name >= 'a' order by id settings allow_experimental_analyzer = 1, prefer_localhost_replica = 0" "--query_id=03682_where_group_by_columns_023"

# Test 9: JOIN query with WHERE and GROUP BY
${CLICKHOUSE_CLIENT} -q "select l.id from (select id,name from test_where_group_by_columns.left_all where log_date <= '2024-05-28' and age = 20 group by id,name) l global join (select * from test_where_group_by_columns.right_all where age > 0) r on l.id = r.id order by l.id settings allow_experimental_analyzer = 0, prefer_localhost_replica = 0" "--query_id=03682_where_group_by_columns_024"
${CLICKHOUSE_CLIENT} -q "select l.id from (select id,name from test_where_group_by_columns.left_all where log_date <= '2024-05-28' and age = 20 group by id,name) l global join (select * from test_where_group_by_columns.right_all where age > 0) r on l.id = r.id order by l.id settings allow_experimental_analyzer = 1, prefer_localhost_replica = 0" "--query_id=03682_where_group_by_columns_025"

# Test 10: Simple SELECT * (no WHERE, no GROUP BY)
${CLICKHOUSE_CLIENT} -q "select * from test_where_group_by_columns.left_all order by id settings allow_experimental_analyzer = 0, prefer_localhost_replica = 0" "--query_id=03682_where_group_by_columns_026"
${CLICKHOUSE_CLIENT} -q "select * from test_where_group_by_columns.left_all order by id settings allow_experimental_analyzer = 1, prefer_localhost_replica = 0" "--query_id=03682_where_group_by_columns_027"

# Cleanup
${CLICKHOUSE_CLIENT} -q "drop database if exists test_where_group_by_columns" "--query_id=03682_where_group_by_columns_028"

# Flush logs and query the results
${CLICKHOUSE_CLIENT} -q "system flush logs"
${CLICKHOUSE_CLIENT} -q "select initial_query_id, where_columns, group_by_columns from system.query_log where type = 'QueryFinish' and initial_query_id like '03682_where_group_by_columns%' and query_id not like '%system%' order by initial_query_id, is_initial_query, tables"
