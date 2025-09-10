-- compact part
DROP TABLE IF EXISTS map_v2_test;
CREATE TABLE map_v2_test
(
    `id` UInt64,
    `m` MapV2(String, String),
    `m_null` MapV2(String, Nullable(String))
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO map_v2_test VALUES (1, {'k1':'v1'}, {'k1':'v1'}), (2, {'k2':'v2'}, {'k2':'v2'}), (3, {'k3':'v3'}, {'k3':NULL});

SELECT sum(length(implicit_columns)) FROM system.parts WHERE table = 'map_v2_test' AND active;

SELECT * FROM map_v2_test SETTINGS enable_analyzer = 1; -- { serverError NOT_IMPLEMENTED }

SELECT m.keys FROM map_v2_test SETTINGS enable_analyzer = 1; -- { serverError NOT_IMPLEMENTED }

SELECT m.values FROM map_v2_test SETTINGS enable_analyzer = 1; -- { serverError NOT_IMPLEMENTED }

SELECT m.size0 FROM map_v2_test SETTINGS enable_analyzer = 1; -- { serverError NOT_IMPLEMENTED }

SELECT m{'k1'}, m{'k2'}, m{'k3'}, m_null{'k1'}, m_null{'k2'}, m_null{'k3'} FROM map_v2_test SETTINGS enable_analyzer = 1;

SELECT * FROM map_v2_test SETTINGS enable_analyzer = 0; -- { serverError NOT_IMPLEMENTED }

SELECT m.keys FROM map_v2_test SETTINGS enable_analyzer = 0; -- { serverError NOT_IMPLEMENTED }

SELECT m.values FROM map_v2_test SETTINGS enable_analyzer = 0; -- { serverError NOT_IMPLEMENTED }

SELECT m.size0 FROM map_v2_test SETTINGS enable_analyzer = 0; -- { serverError NOT_IMPLEMENTED }

SELECT m{'k1'}, m{'k2'}, m{'k3'}, m_null{'k1'}, m_null{'k2'}, m_null{'k3'} FROM map_v2_test SETTINGS enable_analyzer = 0;

INSERT INTO map_v2_test VALUES (4, {'k4':'v4'}, {'k4':NULL}), (5, {'k5':'v5'}, {'k5':'v5'}), (6, {'k6':'v6'}, {'k6':'v6'});

SELECT sum(length(implicit_columns)) FROM system.parts WHERE table = 'map_v2_test' AND active;

OPTIMIZE TABLE map_v2_test FINAL;

SELECT sum(length(implicit_columns)) FROM system.parts WHERE table = 'map_v2_test' AND active;

SELECT m{'k1'}, m{'k2'}, m{'k3'}, m{'k4'}, m{'k5'}, m{'k6'}, m_null{'k1'}, m_null{'k2'}, m_null{'k3'}, m_null{'k4'}, m_null{'k5'}, m_null{'k6'} FROM map_v2_test;

-- wide part
DROP TABLE IF EXISTS map_v2_test;
CREATE TABLE map_v2_test
(
    `id` UInt64,
    `m` MapV2(String, String),
    `m_null` MapV2(String, Nullable(String))
)
ENGINE = MergeTree
ORDER BY id
SETTINGS min_bytes_for_wide_part = 0;

INSERT INTO map_v2_test VALUES (1, {'k1':'v1'}, {'k1':'v1'}), (2, {'k2':'v2'}, {'k2':'v2'}), (3, {'k3':'v3'}, {'k3':NULL});

SELECT sum(length(implicit_columns)) FROM system.parts WHERE table = 'map_v2_test' AND active;

SELECT * FROM map_v2_test SETTINGS enable_analyzer = 1; -- { serverError NOT_IMPLEMENTED }

SELECT m{'k1'}, m{'k2'}, m{'k3'}, m_null{'k1'}, m_null{'k2'}, m_null{'k3'} FROM map_v2_test SETTINGS enable_analyzer = 1;

SELECT * FROM map_v2_test SETTINGS enable_analyzer = 0; -- { serverError NOT_IMPLEMENTED }

SELECT m{'k1'}, m{'k2'}, m{'k3'}, m_null{'k1'}, m_null{'k2'}, m_null{'k3'} FROM map_v2_test SETTINGS enable_analyzer = 0;

INSERT INTO map_v2_test VALUES (4, {'k4':'v4'}, {'k4':NULL}), (5, {'k5':'v5'}, {'k5':'v5'}), (6, {'k6':'v6'}, {'k6':'v6'});

SELECT sum(length(implicit_columns)) FROM system.parts WHERE table = 'map_v2_test' AND active;

OPTIMIZE TABLE map_v2_test FINAL;

SELECT sum(length(implicit_columns)) FROM system.parts WHERE table = 'map_v2_test' AND active;

SELECT m{'k1'}, m{'k2'}, m{'k3'}, m{'k4'}, m{'k5'}, m{'k6'}, m_null{'k1'}, m_null{'k2'}, m_null{'k3'}, m_null{'k4'}, m_null{'k5'}, m_null{'k6'} FROM map_v2_test;

-- implicit_map_duplication
DROP TABLE IF EXISTS map_v2_test;
CREATE TABLE map_v2_test
(
    `id` UInt64,
    `m` MapV2(String, String),
    `m_null` MapV2(String, Nullable(String))
)
ENGINE = MergeTree
ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, implicit_map_duplication = 1;

INSERT INTO map_v2_test VALUES (1, {'k1':'v1'}, {'k1':'v1'}), (2, {'k2':'v2'}, {'k2':'v2'}), (3, {'k3':'v3'}, {'k3':NULL});

SELECT * FROM map_v2_test;

SELECT m{'k1'}, m{'k2'}, m{'k3'}, m_null{'k1'}, m_null{'k2'}, m_null{'k3'} FROM map_v2_test;

INSERT INTO map_v2_test VALUES (4, {'k4':'v4'}, {'k4':NULL}), (5, {'k5':'v5'}, {'k5':'v5'}), (6, {'k6':'v6'}, {'k6':'v6'});

OPTIMIZE TABLE map_v2_test FINAL;

SELECT * FROM map_v2_test;

SELECT m{'k1'}, m{'k2'}, m{'k3'}, m{'k4'}, m{'k5'}, m{'k6'}, m_null{'k1'}, m_null{'k2'}, m_null{'k3'}, m_null{'k4'}, m_null{'k5'}, m_null{'k6'} FROM map_v2_test;

-- secondary indicies
DROP TABLE IF EXISTS map_v2_test;
CREATE TABLE map_v2_test
(
    `id` UInt64,
    `m` MapV2(String, String),
    INDEX idx_m_k1 m{'k1'} TYPE bloom_filter GRANULARITY 1,
    INDEX idx_m_k4 m{'k4'} TYPE bloom_filter GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1;

INSERT INTO map_v2_test VALUES (1, {'k1':'v1'}), (2, {'k2':'v2'}), (3, {'k3':'v3'}), (4, {'k4':'v4'});

SELECT count() FROM map_v2_test WHERE m{'k1'} = 'v1' SETTINGS max_rows_to_read = 1;
SELECT count() FROM map_v2_test WHERE m{'k1'} = 'v2' SETTINGS max_rows_to_read = 1;
SELECT count() FROM map_v2_test WHERE m{'k4'} = 'v3' SETTINGS max_rows_to_read = 1;
SELECT count() FROM map_v2_test WHERE m{'k4'} = 'v4' SETTINGS max_rows_to_read = 1;

DROP TABLE IF EXISTS map_v2_test;

CREATE TABLE map_v2_test
(
    `id` UInt64,
    `m` MapV2(String, String),
    INDEX idx_m_k1 m{'k1'} TYPE bloom_filter GRANULARITY 1,
    INDEX idx_m_k4 m{'k4'} TYPE bloom_filter GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1, min_bytes_for_wide_part = 0;

INSERT INTO map_v2_test VALUES (1, {'k1':'v1'}), (2, {'k2':'v2'}), (3, {'k3':'v3'}), (4, {'k4':'v4'});

SELECT count() FROM map_v2_test WHERE m{'k1'} = 'v1' SETTINGS max_rows_to_read = 1;
SELECT count() FROM map_v2_test WHERE m{'k1'} = 'v2' SETTINGS max_rows_to_read = 1;
SELECT count() FROM map_v2_test WHERE m{'k4'} = 'v3' SETTINGS max_rows_to_read = 1;
SELECT count() FROM map_v2_test WHERE m{'k4'} = 'v4' SETTINGS max_rows_to_read = 1;

-- mutate
DROP TABLE IF EXISTS map_v2_test;
CREATE TABLE map_v2_test
(
    `id` UInt64,
    `m` MapV2(String, String),
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1;

INSERT INTO map_v2_test VALUES (1, {'k1':'v1'}), (2, {'k2':'v2'}), (3, {'k3':'v3'}), (4, {'k4':'v4'});

ALTER TABLE map_v2_test ADD INDEX ix id TYPE minmax GRANULARITY 1;

ALTER TABLE map_v2_test MATERIALIZE INDEX ix SETTINGS mutations_sync = 1;

SELECT m{'k1'}, m{'k2'}, m{'k3'}, m{'k4'} FROM map_v2_test;

ALTER TABLE map_v2_test DROP INDEX ix SETTINGS mutations_sync = 1;

SELECT m{'k1'}, m{'k2'}, m{'k3'}, m{'k4'} FROM map_v2_test;

DROP TABLE IF EXISTS map_v2_test;
CREATE TABLE map_v2_test
(
    `id` UInt64,
    `m` MapV2(String, String),
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1, min_bytes_for_wide_part = 0;

INSERT INTO map_v2_test VALUES (1, {'k1':'v1'}), (2, {'k2':'v2'}), (3, {'k3':'v3'}), (4, {'k4':'v4'});

ALTER TABLE map_v2_test ADD INDEX ix id TYPE minmax GRANULARITY 1;

ALTER TABLE map_v2_test MATERIALIZE INDEX ix SETTINGS mutations_sync = 1;

SELECT m{'k1'}, m{'k2'}, m{'k3'}, m{'k4'} FROM map_v2_test;

ALTER TABLE map_v2_test DROP INDEX ix SETTINGS mutations_sync = 1;

SELECT m{'k1'}, m{'k2'}, m{'k3'}, m{'k4'} FROM map_v2_test;

DROP TABLE IF EXISTS map_v2_test;

CREATE TABLE map_v2_test
(
    `id` UInt64,
    `m` MapV2(String, String),
    INDEX idx_m_k1 m{'k1'} TYPE bloom_filter GRANULARITY 1,
    INDEX idx_m_k4 m{'k4'} TYPE bloom_filter GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1, min_bytes_for_wide_part = 0;

INSERT INTO map_v2_test VALUES (2, {'k2':'v2'});
INSERT INTO map_v2_test VALUES (3, {'k3':'v3'});

OPTIMIZE TABLE map_v2_test FINAL;
