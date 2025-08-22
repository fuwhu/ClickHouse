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
CREATE TABLE test_bsi_agg_functions (id UInt32, bsi BSI) ENGINE=MergeTree() ORDER BY id;


INSERT INTO test_bsi_agg_functions SELECT 1, bsi_build(u_id, gmv) AS bsi FROM
(
    SELECT * FROM
    (
        select 1 as u_id, 2 as gmv
        union all
        select 3 as u_id, 3 as gmv
        union all
        select 2 as u_id, 10 as gmv
    )
    ORDER BY u_id
)
;

INSERT INTO test_bsi_agg_functions SELECT 2, bsi_build(u_id, gmv) AS bsi FROM
(
    SELECT * FROM
    (
        select 1 as u_id, 2 as gmv
        union all
        select 2 as u_id, 10 as gmv
    ) ORDER BY u_id
);

SELECT id, arrayMap(x->bitmapToArray(x), bsi) FROM test_bsi_agg_functions ORDER BY id;
SELECT arrayMap(x->bitmapToArray(x), bsi_add_agg(bsi)) FROM test_bsi_agg_functions;
SELECT bsi_sum(agg) from (select bsi_add_agg(bsi) as agg from test_bsi_agg_functions);
SELECT arrayMap(x->bitmapToArray(x), bsi_merge_agg(bsi)) FROM test_bsi_agg_functions;
SELECT bsi_sum(agg) from (select bsi_merge_agg(bsi) as agg from test_bsi_agg_functions);
DROP TABLE IF EXISTS test_bsi_agg_functions;

SELECT '==========================';

DROP TABLE IF EXISTS test_bsi_zero_metric_functions;
CREATE TABLE test_bsi_zero_metric_functions (id UInt32, bsi BSI) ENGINE=MergeTree() ORDER BY id;


INSERT INTO test_bsi_zero_metric_functions SELECT 1, bsi_build(u_id, gmv) AS bsi FROM
(
    SELECT * FROM
    (
        select 1 as u_id, 0 as gmv
        union all
        select 3 as u_id, 0 as gmv
        union all
        select 2 as u_id, 0 as gmv
    )
    ORDER BY u_id
);

INSERT INTO test_bsi_zero_metric_functions SELECT 2, bsi_build(u_id, gmv) AS bsi FROM
(
    SELECT * FROM
    (
        select 1 as u_id, 0 as gmv
        union all
        select 2 as u_id, 0 as gmv
        union all
        select 5 as u_id, 0 as gmv
    )
    ORDER BY u_id
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



SELECT '==========================';

DROP TABLE IF EXISTS user_click_detail;
DROP TABLE IF EXISTS user_click_bsi;

CREATE TABLE user_click_detail
(
    `u_id` UInt64,
    `sex` String,
    `city` String,
    `click` UInt64
)
ENGINE = MergeTree
ORDER BY u_id;

INSERT INTO user_click_detail VALUES (1, 'woman', 'sh', 1048576), (2, 'man', 'sh', 3), (3, 'woman', 'bj', 6), (4, 'man', 'sh', 8), (5, 'woman', 'sh', 8), (6, 'man', 'bj', 5);

SELECT * FROM user_click_detail ORDER BY u_id ASC;

CREATE TABLE user_click_bsi
(
    `no` UInt32,
    `tag_name` Enum('sex' = 1, 'city' = 2),
    `tag_value` String,
    `click_bsi` BSI
)
ENGINE = MergeTree
ORDER BY tuple();

INSERT INTO user_click_bsi SELECT 1, 'sex', sex, bsi_build(u_id, click) FROM user_click_detail group by sex;
INSERT INTO user_click_bsi SELECT 2, 'city', city, bsi_build(u_id, click) FROM user_click_detail group by city;

SELECT
    no,
    tag_name,
    tag_value,
    arrayMap(x -> bitmapToArray(x), bsi) AS arr_id,
    arrayMap(x -> bitmapCardinality(x), bsi) AS arr_cardinality
FROM
(
    SELECT
        no,
        tag_name,
        tag_value,
        click_bsi AS bsi
    FROM user_click_bsi
    ORDER BY
        no ASC,
        tag_name ASC,
        tag_value ASC
);

WITH 
(select click_bsi from user_click_bsi where tag_name = 'sex' and tag_value = 'woman') as sex_man_bsi,
(select click_bsi from user_click_bsi where tag_name = 'city' and tag_value = 'sh') as city_sh_bsi
SELECT bsi_product_sum(sex_man_bsi, city_sh_bsi);

WITH 
(select click_bsi from user_click_bsi where tag_name = 'sex' and tag_value = 'woman') as sex_man_bsi
SELECT `no`, tag_name, tag_value, bsi_product_sum(click_bsi, sex_man_bsi) from user_click_bsi order by `no`, tag_name, tag_value;

WITH 
(select click_bsi from user_click_bsi where tag_name = 'sex' and tag_value = 'woman') as sex_man_bsi
SELECT bsi_product_sum(sex_man_bsi, sex_man_bsi);

SELECT `no`, tag_name, tag_value, bsi_product_sum(click_bsi, click_bsi) from user_click_bsi order by `no`, tag_name, tag_value;

SELECT bsi_product_sum([bitmapBuild([cast(1, 'UInt64'), cast(2, 'UInt64'), cast(3, 'UInt64'), cast(4, 'UInt64')]), bitmapBuild([cast(1, 'UInt64'), cast(2, 'UInt64'), cast(3, 'UInt64'), cast(4, 'UInt64')]), bitmapBuild([cast(2, 'UInt64'), cast(3, 'UInt64')]), bitmapBuild([cast(4, 'UInt64')])], [bitmapBuild([cast(1, 'UInt64'), cast(2, 'UInt64'), cast(3, 'UInt64'), cast(4, 'UInt64')]), bitmapBuild([cast(1, 'UInt64'), cast(2, 'UInt64'), cast(3, 'UInt64'), cast(4, 'UInt64')]), bitmapBuild([cast(2, 'UInt64'), cast(3, 'UInt64')]), bitmapBuild([cast(4, 'UInt64')]), bitmapBuild([cast(4, 'UInt64')])]);

SELECT bsi_product_sum([bitmapBuild([cast(1, 'UInt64'), cast(2, 'UInt64'), cast(3, 'UInt64'), cast(4, 'UInt64')]), bitmapBuild([cast(1, 'UInt64'), cast(2, 'UInt64'), cast(3, 'UInt64'), cast(4, 'UInt64')]), bitmapBuild([cast(2, 'UInt64'), cast(3, 'UInt64')]), bitmapBuild([cast(4, 'UInt64')])], [bitmapBuild([cast(1, 'UInt64'), cast(2, 'UInt64'), cast(3, 'UInt64'), cast(4, 'UInt64')]), bitmapBuild([cast(1, 'UInt64'), cast(2, 'UInt64'), cast(3, 'UInt64'), cast(4, 'UInt64')]), bitmapBuild([cast(2, 'UInt64'), cast(3, 'UInt64')]), bitmapBuild([cast(4, 'UInt64')])]);


WITH 
(select click_bsi from user_click_bsi where tag_name = 'sex' and tag_value = 'woman') as sex_man_bsi
SELECT bsi_square_sum(sex_man_bsi);

SELECT `no`, tag_name, tag_value, bsi_square_sum(click_bsi) from user_click_bsi order by `no`, tag_name, tag_value;

SELECT bsi_square_sum([bitmapBuild([cast(1, 'UInt64'), cast(2, 'UInt64'), cast(3, 'UInt64'), cast(4, 'UInt64')]), bitmapBuild([cast(1, 'UInt64'), cast(2, 'UInt64'), cast(3, 'UInt64'), cast(4, 'UInt64')]), bitmapBuild([cast(2, 'UInt64'), cast(3, 'UInt64')]), bitmapBuild([cast(4, 'UInt64')])]);

DROP TABLE IF EXISTS user_click_detail;
DROP TABLE IF EXISTS user_click_bsi;


SELECT '==========================';

DROP TABLE IF EXISTS test_bsi_agg_empty_result;

CREATE TABLE test_bsi_agg_empty_result
(
    `id` UInt32,
    `key` String,
    `value` UInt32, 
    `bsi` BSI
)
ENGINE = MergeTree
ORDER BY tuple();

INSERT INTO TABLE test_bsi_agg_empty_result values (1, 'a', 10, [bitmapBuild([1, 2, 3, 4]), bitmapBuild([1, 2, 3, 4]), bitmapBuild([2, 3]), bitmapBuild([4])]);
INSERT INTO TABLE test_bsi_agg_empty_result values (2, 'b', 20, [bitmapBuild([3, 5, 7]), bitmapBuild([3]), bitmapBuild([3, 5]), bitmapBuild([7])]);
INSERT INTO TABLE test_bsi_agg_empty_result values (1, 'c', 30, [bitmapBuild([1, 2, 5]), bitmapBuild([1, 2, 5]), bitmapBuild([1, 2]), bitmapBuild([5])]);

SELECT
    bsi_sum(bsi_add_aggIf(bsi, id = 1)),
    bsi_sum(bsi_add_aggIf(bsi, id = 3))
FROM test_bsi_agg_empty_result;

SELECT
    bsi_sum(bsi_merge_aggIf(bsi, id = 1)),
    bsi_sum(bsi_merge_aggIf(bsi, id = 3))
FROM test_bsi_agg_empty_result;

SELECT
    bsi_sum(bsi_buildIf(id, value, id = 2)),
    bsi_sum(bsi_buildIf(id, value, id = 3))
FROM test_bsi_agg_empty_result;

DROP TABLE IF EXISTS test_bsi_agg_empty_result;
