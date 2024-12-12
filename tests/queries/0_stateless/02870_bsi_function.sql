DROP TABLE IF EXISTS test_bsi_functions;
CREATE TABLE test_bsi_functions (id UInt32, bsi BSI) ENGINE=MergeTree() ORDER BY tuple();

INSERT INTO TABLE test_bsi_functions values (1, [bitmapBuild([1, 2, 3, 4]), bitmapBuild([1, 2, 3, 4]), bitmapBuild([2, 3]), bitmapBuild([4])]);
SELECT id, arrayMap(x->bitmapToArray(x), bsi) FROM test_bsi_functions ORDER BY id;
SELECT bitmapToArray(bsi_lt(bsi, 2)) FROM test_bsi_functions;
SELECT bitmapToArray(bsi_le(bsi, 2)) FROM test_bsi_functions;
SELECT bitmapToArray(bsi_gt(bsi, 2)) FROM test_bsi_functions;
SELECT bitmapToArray(bsi_ge(bsi, 2)) FROM test_bsi_functions;
SELECT bitmapToArray(bsi_range(bsi, 1, 4)) FROM test_bsi_functions;
SELECT t.res.1 FROM (SELECT arrayJoin(bsi_topk(bsi, 2)) as res FROM test_bsi_functions ORDER BY res.2 desc LIMIT 2) as t;

INSERT INTO TABLE test_bsi_functions values (2, [bitmapBuild([2, 3, 4, 5]), bitmapBuild([3, 4]), bitmapBuild([2, 5]), bitmapBuild([2])]);
SELECT id, arrayMap(x->bitmapToArray(x), bsi) FROM test_bsi_functions ORDER BY id;
SELECT arrayMap(x->bitmapToArray(x), bsi_add_agg(bsi)) FROM test_bsi_functions;
DROP TABLE IF EXISTS test_bsi_functions;

SELECT '==========================';

DROP TABLE IF EXISTS test_bsi_agg_functions;
CREATE TABLE test_bsi_agg_functions (id UInt32, bsi BSI) ENGINE=MergeTree() ORDER BY tuple();


INSERT INTO test_bsi_agg_functions SELECT 1, bsi_build(u_id, gmv) AS bsi FROM
(
    select 1 as u_id, 2 as gmv
    union all
    select 3 as u_id, 3 as gmv
    union all
    select 2 as u_id, 10 as gmv
);

INSERT INTO test_bsi_agg_functions SELECT 2, bsi_build(u_id, gmv) AS bsi FROM
(
    select 1 as u_id, 2 as gmv
    union all
    select 2 as u_id, 10 as gmv
);

SELECT id, arrayMap(x->bitmapToArray(x), bsi) FROM test_bsi_agg_functions ORDER BY id;
SELECT arrayMap(x->bitmapToArray(x), bsi_add_agg(bsi)) FROM test_bsi_agg_functions;
SELECT bsi_sum(agg) from (select bsi_add_agg(bsi) as agg from test_bsi_agg_functions);
SELECT arrayMap(x->bitmapToArray(x), bsi_merge_agg(bsi)) FROM test_bsi_agg_functions;
SELECT bsi_sum(agg) from (select bsi_merge_agg(bsi) as agg from test_bsi_agg_functions);
DROP TABLE IF EXISTS test_bsi_agg_functions;

SELECT '==========================';

DROP TABLE IF EXISTS test_bsi_zero_metric_functions;
CREATE TABLE test_bsi_zero_metric_functions (id UInt32, bsi BSI) ENGINE=MergeTree() ORDER BY tuple();


INSERT INTO test_bsi_zero_metric_functions SELECT 1, bsi_build(u_id, gmv) AS bsi FROM
(
    select 1 as u_id, 0 as gmv
    union all
    select 3 as u_id, 0 as gmv
    union all
    select 2 as u_id, 0 as gmv
);

INSERT INTO test_bsi_zero_metric_functions SELECT 2, bsi_build(u_id, gmv) AS bsi FROM
(
    select 1 as u_id, 0 as gmv
    union all
    select 2 as u_id, 0 as gmv
    union all
    select 5 as u_id, 0 as gmv
);

SELECT id, arrayMap(x->bitmapToArray(x), bsi) FROM test_bsi_zero_metric_functions ORDER BY id;
SELECT arrayMap(x->bitmapToArray(x), bsi_add_agg(bsi)) FROM test_bsi_zero_metric_functions;
SELECT bsi_sum(agg) from (select bsi_add_agg(bsi) as agg from test_bsi_zero_metric_functions);
SELECT arrayMap(x->bitmapToArray(x), bsi_merge_agg(bsi)) FROM test_bsi_zero_metric_functions;
SELECT bsi_sum(agg) from (select bsi_merge_agg(bsi) as agg from test_bsi_zero_metric_functions);
DROP TABLE IF EXISTS test_bsi_zero_metric_functions;

SELECT '==========================';

SELECT bitmapToArray(bsi_range([bitmapBuild([cast(1, 'UInt64'), cast(2, 'UInt64'), cast(3, 'UInt64')]), bitmapBuild([cast(3, 'UInt64')]), bitmapBuild([cast(1, 'UInt64'), cast(3, 'UInt64')])], 0, 3));
SELECT bitmapToArray(bsi_range([bitmapBuild([cast(1, 'UInt64'), cast(2, 'UInt64'), cast(3, 'UInt64')]), bitmapBuild([cast(3, 'UInt64')]), bitmapBuild([cast(1, 'UInt64'), cast(3, 'UInt64')])], 0, 1));
SELECT bitmapToArray(bsi_range([bitmapBuild([cast(1, 'UInt64'), cast(2, 'UInt64'), cast(3, 'UInt64')]), bitmapBuild([cast(3, 'UInt64')]), bitmapBuild([cast(1, 'UInt64'), cast(3, 'UInt64')])], 0, 2));
SELECT bitmapToArray(bsi_range([bitmapBuild([cast(1, 'UInt64'), cast(2, 'UInt64'), cast(3, 'UInt64')]), bitmapBuild([cast(3, 'UInt64')]), bitmapBuild([cast(1, 'UInt64'), cast(3, 'UInt64')])], 5, 3));
SELECT bitmapToArray(bsi_range([bitmapBuild([cast(1, 'UInt64'), cast(2, 'UInt64'), cast(3, 'UInt64')]), bitmapBuild([cast(3, 'UInt64')]), bitmapBuild([cast(1, 'UInt64'), cast(3, 'UInt64')])], 10, 20));
SELECT bitmapToArray(bsi_range([bitmapBuild([cast(1, 'UInt64'), cast(2, 'UInt64'), cast(3, 'UInt64')]), bitmapBuild([cast(3, 'UInt64')]), bitmapBuild([cast(1, 'UInt64'), cast(3, 'UInt64')])], 0, 100));
SELECT bitmapToArray(bsi_gt([bitmapBuild([cast(1, 'UInt64'), cast(2, 'UInt64'), cast(3, 'UInt64')]), bitmapBuild([cast(3, 'UInt64')]), bitmapBuild([cast(1, 'UInt64'), cast(3, 'UInt64')])], 0));
SELECT bitmapToArray(bsi_ge([bitmapBuild([cast(1, 'UInt64'), cast(2, 'UInt64'), cast(3, 'UInt64')]), bitmapBuild([cast(3, 'UInt64')]), bitmapBuild([cast(1, 'UInt64'), cast(3, 'UInt64')])], 0));
SELECT bitmapToArray(bsi_lt([bitmapBuild([cast(1, 'UInt64'), cast(2, 'UInt64'), cast(3, 'UInt64')]), bitmapBuild([cast(3, 'UInt64')]), bitmapBuild([cast(1, 'UInt64'), cast(3, 'UInt64')])], 0));
SELECT bitmapToArray(bsi_le([bitmapBuild([cast(1, 'UInt64'), cast(2, 'UInt64'), cast(3, 'UInt64')]), bitmapBuild([cast(3, 'UInt64')]), bitmapBuild([cast(1, 'UInt64'), cast(3, 'UInt64')])], 0));
