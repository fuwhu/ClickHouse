DROP TABLE IF EXISTS user_gmv_detail;
DROP TABLE IF EXISTS user_gmv_bsi;

CREATE TABLE user_gmv_detail
(
    `u_id` UInt64,
    `gmv` UInt64
)
ENGINE = MergeTree
ORDER BY tuple();

INSERT INTO user_gmv_detail VALUES (1, 3), (2, 6), (3, 4), (4, 10), (5, 7);

SELECT * FROM user_gmv_detail ORDER BY u_id ASC;

SELECT 
    arrayMap(x -> arraySort(bitmapToArray(x)), bsi) AS arr_id,
    arrayMap(x -> bitmapCardinality(x), bsi) AS arr_cardinality
FROM
(
    SELECT bsi_build(u_id, gmv) AS bsi FROM user_gmv_detail
);

CREATE TABLE user_gmv_bsi
(
    `bsi` BSI
)
ENGINE = MergeTree
ORDER BY tuple();

INSERT INTO user_gmv_bsi SELECT bsi_build(u_id, gmv) AS bsi FROM user_gmv_detail;

SELECT bsi_sum(bsi) from user_gmv_bsi;

SELECT bsi_sum(bsi) AS sum_cnt, (sum_cnt.1) / (sum_cnt.2) AS avg FROM user_gmv_bsi;

SELECT bsi_sum(bsi, bitmapBuild([CAST(1, 'UInt64'), CAST(5, 'UInt64')])) FROM user_gmv_bsi;

SELECT 
    arrayMap(x -> arraySort(bitmapToArray(x)), bsi) AS arr_id,
    arrayMap(x -> bitmapCardinality(x), bsi) AS arr_cardinality
FROM
(
    SELECT bsi_filter(bsi, bitmapBuild([CAST(1, 'UInt64'), CAST(5, 'UInt64')])) as bsi FROM user_gmv_bsi
);

SELECT 
    bsi_sum(bsi)
FROM
(
    SELECT bsi_filter(bsi, bitmapBuild([CAST(1, 'UInt64'), CAST(5, 'UInt64')])) as bsi FROM user_gmv_bsi
);

DROP TABLE IF EXISTS user_gmv_bsi;
DROP TABLE IF EXISTS user_gmv_detail;
