DROP TABLE IF EXISTS test_token_like;
CREATE TABLE test_token_like (`id` UInt32, `msg` String, `dt` Date, INDEX msg_bf msg TYPE tokenbf_v1(512, 3, 0) GRANULARITY 1) ENGINE = MergeTree PARTITION BY dt ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO test_token_like VALUES (1, 'has exceptions', '2023-08-10'), (2, 'nothas exceptions', '2023-08-10'), (3, 'not hasexceptions', '2023-08-10');
SELECT msg FROM test_token_like WHERE msg like '%has%';
SELECT msg FROM test_token_like WHERE tokenLike(msg, '%has%');
SELECT msg FROM test_token_like WHERE msg like '%exceptions';
SELECT msg FROM test_token_like WHERE tokenLike(msg, '%exceptions');
DROP TABLE test_token_like;
