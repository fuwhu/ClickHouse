-- Tags: no-parallel	
DROP TABLE IF EXISTS test_z_order;
CREATE TABLE test_z_order (x String, y String) ENGINE = MergeTree() ORDER BY (x, y) SETTINGS order_by_use_zcurve = 1;
INSERT INTO test_z_order VALUES ('0','0'), ('0','1'), ('0','2'), ('0','3'), ('1','0'), ('1','1'), ('1','2'), ('1','3'), ('2','0'), ('2','1'), ('2','2'), ('2','3'), ('3','0'), ('3','1'), ('3','2'), ('3','3');
OPTIMIZE TABLE test_z_order FINAL;
SELECT * FROM test_z_order;
SELECT * FROM test_z_order ORDER BY x ASC;
SELECT * FROM test_z_order ORDER BY x DESC;
DROP TABLE test_z_order;

CREATE TABLE test_z_order (x UInt64, y UInt64) ENGINE = MergeTree() ORDER BY (x, y) SETTINGS order_by_use_zcurve = 1;
INSERT INTO test_z_order VALUES (0, 0), (0, 1), (0, 2), (0, 3), (1, 0), (1, 1), (1, 2), (1, 3), (2, 0), (2, 1), (2, 2), (2, 3), (3, 0), (3, 1), (3, 2), (3, 3);
OPTIMIZE TABLE test_z_order FINAL;
SELECT * FROM test_z_order;
SELECT * FROM test_z_order ORDER BY x ASC;
SELECT * FROM test_z_order ORDER BY x DESC;
DROP TABLE test_z_order;

CREATE TABLE test_z_order
(
    `x` String,
    `y` String,
    INDEX ix x TYPE minmax GRANULARITY 1,
    INDEX iy y TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY (x, y)
SETTINGS order_by_use_zcurve = 1, index_granularity = 2;
INSERT INTO test_z_order VALUES ('0','0'), ('0','1'), ('0','2'), ('0','3'), ('1','0'), ('1','1'), ('1','2'), ('1','3'), ('2','0'), ('2','1'), ('2','2'), ('2','3'), ('3','0'), ('3','1'), ('3','2'), ('3','3');
OPTIMIZE TABLE test_z_order FINAL;
SELECT count() FROM test_z_order WHERE x = '2' SETTINGS max_rows_to_read = 8;
SELECT count() FROM test_z_order WHERE y = '2' SETTINGS max_rows_to_read = 4;
DROP TABLE test_z_order;