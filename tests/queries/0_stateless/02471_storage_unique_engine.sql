DROP TABLE IF EXISTS test_unique_engine;

CREATE TABLE test_unique_engine
(
    `id` UInt64,
    `name` String,
    `version` DateTime
)
ENGINE = UniqueMergeTree(version)
ORDER BY id
UNIQUE KEY name
SETTINGS unique_key_index_type = 1;

INSERT INTO test_unique_engine VALUES ('1', 'Jack', '2023-08-21 10:00:00'), ('2', 'Leo', '2023-08-21 10:00:00');

SELECT * FROM test_unique_engine ORDER BY id ASC;

INSERT INTO test_unique_engine VALUES ('2', 'Leo', '2023-08-21 10:05:00'), ('2', 'Leo', '2023-08-21 10:10:00');

SELECT * FROM test_unique_engine ORDER BY id ASC;

INSERT INTO test_unique_engine VALUES ('3', 'Rachel', '2023-08-21 10:00:00'), ('1', 'Jack', '2023-08-21 09:00:00');

SYSTEM FLUSH LOGS query_log;
SELECT ProfileEvents['UniqueMergeTreeDedupComparedParts'], ProfileEvents['UniqueMergeTreeDedupPartsWithDuplicates'] FROM system.query_log WHERE event_date = today() and type != 1 and query_kind = 'Insert' and query like '%test_unique_engine%' order by event_time_microseconds DESC;

SELECT * FROM test_unique_engine ORDER BY id ASC;

DROP TABLE IF EXISTS test_unique_engine;

DROP TABLE IF EXISTS test_unique_bitmap_filter;

CREATE TABLE test_unique_bitmap_filter
(
    `id` UInt64,
    `num` Int64,
    `type` Nullable(UInt16),
    `ctime` DateTime,
    `mtime` DateTime
)
ENGINE = UniqueMergeTree(mtime)
ORDER BY id
UNIQUE KEY id
SETTINGS unique_key_index_type = 3;

INSERT INTO test_unique_bitmap_filter VALUES (1, 10, 0,'2024-07-29 11:58:00', '2024-07-29 11:58:00'), (2, 20, 1, '2024-07-29 11:58:00', '2024-07-29 11:58:00');
INSERT INTO test_unique_bitmap_filter VALUES (1, 10, 2, '2024-07-29 11:58:00', '2024-07-29 12:00:00'), (3, 20, null, '2024-07-29 12:00:00', '2024-07-29 12:00:00');
INSERT INTO test_unique_bitmap_filter VALUES (1, 10, 3, '2024-07-29 11:58:00', '2024-07-29 12:10:00'), (4, 20, 10, '2024-07-29 12:10:00', '2024-07-29 12:10:00');

SELECT
    *,
    multiIf((num >= 0) AND (num <= 100), '0-100', (num >= 100) AND (num <= 1000), '100-1k', (num >= 1000) AND (num <= 10000), '1k-1w', (num >= 10000) AND (num <= 30000), '1w-3w', (num >= 30000) AND (num <= 100000), '3w-10w', (num >= 100000) AND (num <= 500000), '10w-50w', (num >= 500000) AND (num <= 1000000), '50w-100w', (num >= 1000000) AND (num <= 5000000), '100w-500w', num > 5000000, '500w+', NULL) AS level
FROM test_unique_bitmap_filter
WHERE level IN ('0-100')
ORDER BY id
SETTINGS max_threads = 1;

SELECT
    *,
    multiIf((num >= 0) AND (num <= 100), '0-100', (num >= 100) AND (num <= 1000), '100-1k', (num >= 1000) AND (num <= 10000), '1k-1w', (num >= 10000) AND (num <= 30000), '1w-3w', (num >= 30000) AND (num <= 100000), '3w-10w', (num >= 100000) AND (num <= 500000), '10w-50w', (num >= 500000) AND (num <= 1000000), '50w-100w', (num >= 1000000) AND (num <= 5000000), '100w-500w', num > 5000000, '500w+', NULL) AS level
FROM test_unique_bitmap_filter
WHERE num > 10
ORDER BY id
SETTINGS max_threads = 1;

SELECT
    *,
    multiIf((num >= 0) AND (num <= 100), '0-100', (num >= 100) AND (num <= 1000), '100-1k', (num >= 1000) AND (num <= 10000), '1k-1w', (num >= 10000) AND (num <= 30000), '1w-3w', (num >= 30000) AND (num <= 100000), '3w-10w', (num >= 100000) AND (num <= 500000), '10w-50w', (num >= 500000) AND (num <= 1000000), '50w-100w', (num >= 1000000) AND (num <= 5000000), '100w-500w', num > 5000000, '500w+', NULL) AS level
FROM test_unique_bitmap_filter
WHERE type is not null
ORDER BY id
SETTINGS max_threads = 1;

DROP TABLE IF EXISTS test_unique_bitmap_filter;

DROP TABLE IF EXISTS test_uk_level_db_test_str;

CREATE TABLE test_uk_level_db_test_str
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

INSERT INTO test_uk_level_db_test_str VALUES (388091629, 1006, '2d88ff16-77de-4d03-811f-07f792e9aead', 10, '33989', 1, '2024-08-05 10:00:00');

DETACH TABLE test_uk_level_db_test_str;

ATTACH TABLE test_uk_level_db_test_str;

INSERT INTO test_uk_level_db_test_str VALUES (388091629, 1006, '2d88ff16-77de-4d03-811f-07f792e9aead', 10, '33989', 2, '2024-08-05 10:00:00');

SELECT * FROM test_uk_level_db_test_str;

DROP TABLE IF EXISTS test_uk_level_db_test_str;


DROP TABLE IF EXISTS test_uk_partition_lock;

CREATE TABLE test_uk_partition_lock
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
SETTINGS unique_key_deduplicate_level = 1;

INSERT INTO test_uk_partition_lock VALUES (1, 1006, '2d88ff16-77de-4d03-811f-07f792e9aead', 10, '33989', 1, '2024-12-30 10:00:00');

INSERT INTO test_uk_partition_lock VALUES (2, 1006, '2d88ff16-77de-4d03-811f-07f792e9aead', 10, '33989', 2, '2024-12-31 10:00:00');

INSERT INTO test_uk_partition_lock VALUES (3, 1006, '2d88ff16-77de-4d03-811f-07f792e9aead', 10, '33989', 3, '2024-12-30 10:00:00');

INSERT INTO test_uk_partition_lock VALUES (4, 1006, '2d88ff16-77de-4d03-811f-07f792e9aead', 10, '33989', 4, '2024-12-31 10:00:00');

SELECT * FROM test_uk_partition_lock ORDER BY uid;

DROP TABLE IF EXISTS test_uk_partition_lock;

DROP TABLE IF EXISTS test_uk_settings;

CREATE TABLE test_uk_settings
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
SETTINGS unique_key_index_type = 5; -- { serverError BAD_ARGUMENTS }

DROP TABLE IF EXISTS test_uk_settings;


DROP TABLE IF EXISTS test_unique_engine_merge;

CREATE TABLE test_unique_engine_merge (id UInt32, name String, version UInt32, dt DateTime)
ENGINE = UniqueMergeTree(version)
PARTITION BY toDate(dt)
ORDER BY id
UNIQUE KEY id
SETTINGS index_granularity = 3, index_granularity_bytes = 0, min_bytes_for_wide_part = 0, max_bytes_to_merge_at_min_space_in_pool = 0, max_bytes_to_merge_at_max_space_in_pool = 0;

INSERT INTO test_unique_engine_merge VALUES (1, 'a', 1, '2025-11-11 15:00:00'), (2, 'b2', 2, '2025-11-11 15:00:00'), (3, 'c', 1, '2025-11-11 15:00:00'),  (1, 'a2', 2, '2025-11-11 15:00:00'), (2, 'b', 1, '2025-11-11 15:00:00'), (5, 'e', 1, '2025-11-11 15:00:00'), (6, 'f', 1, '2025-11-11 15:00:00');

INSERT INTO test_unique_engine_merge VALUES (7, 'g', 1, '2025-11-11 15:00:00');

SELECT _part, * FROM test_unique_engine_merge ORDER BY id;

OPTIMIZE table test_unique_engine_merge FINAL;

SELECT _part, * FROM test_unique_engine_merge ORDER BY id;

DROP TABLE IF EXISTS test_unique_engine_merge;
