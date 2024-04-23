CREATE TABLE t
(
    `id` UInt64,
    INDEX id_idx id TYPE bloom_filter GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO t FORMAT Values (1);

WITH [1] AS a
SELECT *
FROM t
WHERE has(a, id);
