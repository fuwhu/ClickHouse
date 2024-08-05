-- Tags: no-parallel

DROP DATABASE IF EXISTS test_02871;
CREATE DATABASE test_02871 ENGINE=Ordinary;


DROP TABLE IF EXISTS test_02871.uk_level_db_test_str;
CREATE TABLE test_02871.uk_level_db_test_str
(
    `uid` Int64,
    `send_source` Int64,
    `msg_id` String,
    `award_type` Int64,
    `award_id` String,
    `version` Int64,
    `ctime` DateTime
)
ENGINE = UniqueMergeTree(version)
PARTITION BY toDate(ctime)
ORDER BY award_id
UNIQUE KEY award_id
SETTINGS unique_key_index_type = 3, unique_delete_bitmap_type = 32, unique_key_deduplicate_level = 1;

INSERT INTO test_02871.uk_level_db_test_str VALUES (388091629, 1006, '2d88ff16-77de-4d03-811f-07f792e9aead', 10, '33989', 1, '2024-08-05 10:00:00');

INSERT INTO test_02871.uk_level_db_test_str VALUES (388091629, 1006, '2d88ff16-77de-4d03-811f-07f792e9aead', 10, '33989', 1, '2024-08-05 10:00:00');


DROP TABLE IF EXISTS test_02871.uk_level_db_test_int;
CREATE TABLE test_02871.uk_level_db_test_int
(
    `uid` Int64,
    `send_source` Int64,
    `msg_id` String,
    `award_type` Int64,
    `award_id` Int64,
    `version` Int64,
    `ctime` DateTime
)
ENGINE = UniqueMergeTree(version)
PARTITION BY toDate(ctime)
ORDER BY award_id
UNIQUE KEY award_id
SETTINGS unique_key_index_type = 3, unique_delete_bitmap_type = 32, unique_key_deduplicate_level = 1;

INSERT INTO test_02871.uk_level_db_test_int VALUES (388091629, 1006, '2d88ff16-77de-4d03-811f-07f792e9aead', 10, 33989, 1, '2024-08-05 10:00:00');

INSERT INTO test_02871.uk_level_db_test_int VALUES (388091629, 1006, '2d88ff16-77de-4d03-811f-07f792e9aead', 10, 33989, 1, '2024-08-05 10:00:00');

INSERT INTO test_02871.uk_level_db_test_int VALUES (388091629, 1006, '2d88ff16-77de-4d03-811f-07f792e9aead', 10, 33989, 1, '2024-08-05 10:00:00');


SELECT * FROM test_02871.uk_level_db_test_int;

DROP TABLE IF EXISTS test_02871.uk_level_db_test_int;
DROP TABLE IF EXISTS test_02871.uk_level_db_test_str;

DROP DATABASE IF EXISTS test_02871;