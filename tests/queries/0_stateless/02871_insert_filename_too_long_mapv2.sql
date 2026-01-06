DROP TABLE IF EXISTS test_mapv2;

CREATE TABLE test_mapv2
(
    `id` UInt32,
    `string_map` MapV2(String, String),
    `dt` Date
)
ENGINE = MergeTree
PARTITION BY dt
ORDER BY id
SETTINGS index_granularity = 8192, replace_long_file_name_to_hash = 1, max_file_name_length = 42, min_bytes_for_wide_part = 0;

INSERT INTO TABLE test_mapv2 VALUES (1, {'kkkkssdasadfadsfadfadfadfad': 'v1', 'kkkkssdasadfadsfadfadfadfasdfasfgbchhkkkkssdasadfadsfadfadfadfasdfasfgbchh': 'v2'}, today());

SELECT string_map{'kkkkssdasadfadsfadfadfadfad'} FROM test_mapv2;

SELECT string_map{'kkkkssdasadfadsfadfadfadfasdfasfgbchhkkkkssdasadfadsfadfadfadfasdfasfgbchh'} FROM test_mapv2;

SELECT implicit_columns
FROM system.parts
WHERE table = 'test_mapv2';

ALTER TABLE test_mapv2 ADD COLUMN test_col UInt32;

ALTER TABLE test_mapv2 RENAME COLUMN test_col TO f8fc46b1b59c6e8a8fb40c5951717f83; -- { serverError DUPLICATE_COLUMN }

ALTER TABLE test_mapv2 ADD COLUMN f8fc46b1b59c6e8a8fb40c5951717f83 UInt32; -- { serverError DUPLICATE_COLUMN }

DROP TABLE test_mapv2;
