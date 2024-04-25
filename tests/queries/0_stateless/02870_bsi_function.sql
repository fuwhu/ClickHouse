DROP TABLE IF EXISTS test_bsi_functions;
CREATE TABLE test_bsi_functions (bsi BSI) ENGINE=MergeTree() ORDER BY tuple();

INSERT INTO TABLE test_bsi_functions values ([bitmapBuild([1, 2, 3, 4]), bitmapBuild([1, 2, 3, 4]), bitmapBuild([2, 3]), bitmapBuild([4])]);
SELECT arrayMap(x->bitmapToArray(x), bsi) FROM test_bsi_functions;
SELECT bitmapToArray(bsi_lt(bsi, 2)) FROM test_bsi_functions;
SELECT bitmapToArray(bsi_le(bsi, 2)) FROM test_bsi_functions;
SELECT bitmapToArray(bsi_gt(bsi, 2)) FROM test_bsi_functions;
SELECT bitmapToArray(bsi_ge(bsi, 2)) FROM test_bsi_functions;
SELECT bitmapToArray(bsi_range(bsi, 1, 4)) FROM test_bsi_functions;
SELECT t.res.1 FROM (SELECT arrayJoin(bsi_topk(bsi, 2)) as res FROM test_bsi_functions ORDER BY res.2 desc LIMIT 2) as t;

INSERT INTO TABLE test_bsi_functions values ([bitmapBuild([2, 3, 4, 5]), bitmapBuild([3, 4]), bitmapBuild([2, 5]), bitmapBuild([2])]);
SELECT arrayMap(x->bitmapToArray(x), bsi) FROM test_bsi_functions;
SELECT arrayMap(x->bitmapToArray(x), bsi_add_agg(bsi)) FROM test_bsi_functions;
DROP TABLE IF EXISTS test_bsi_functions;