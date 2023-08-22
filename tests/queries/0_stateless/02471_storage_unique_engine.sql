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

SELECT * FROM test_unique_engine ORDER BY id ASC;

DROP TABLE IF EXISTS test_unique_engine;
