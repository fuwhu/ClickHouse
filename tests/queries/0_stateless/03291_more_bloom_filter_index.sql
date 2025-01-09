DROP TABLE IF EXISTS bloom_filter_test;

CREATE TABLE bloom_filter_test(id UInt64, a Array(String), m Map(String, String), INDEX idx_a a type bloom_filter GRANULARITY 1, INDEX idx_m m type bloom_filter GRANULARITY 1) ENGINE = MergeTree() ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO bloom_filter_test VALUES (1, ['1'], {'1': '1'}), (2, ['2'], {'2': '2'}), (3, ['3'], {'3': '3'});

SELECT count() FROM bloom_filter_test WHERE a[1] = '1' SETTINGS max_rows_to_read = 1, force_data_skipping_indices = 'idx_a';
SELECT count() FROM bloom_filter_test WHERE a[1] = '4' SETTINGS max_rows_to_read = 1, force_data_skipping_indices = 'idx_a';
SELECT count() FROM bloom_filter_test WHERE a[1] = '' SETTINGS max_rows_to_read = 1, force_data_skipping_indices = 'idx_a';
SELECT count() FROM bloom_filter_test WHERE a[1] >= '1';
SELECT count() FROM bloom_filter_test WHERE a[1] <= '1';

-- should disable new analyzer now because of some bugs
SELECT count() FROM bloom_filter_test WHERE mapContains(m, '1') SETTINGS max_rows_to_read = 1, force_data_skipping_indices = 'idx_m', enable_analyzer = 0;
SELECT count() FROM bloom_filter_test WHERE mapContains(m, '4') SETTINGS max_rows_to_read = 1, force_data_skipping_indices = 'idx_m', enable_analyzer = 0;

SELECT count() FROM bloom_filter_test WHERE has(m, '2') SETTINGS max_rows_to_read = 1, force_data_skipping_indices = 'idx_m';
SELECT count() FROM bloom_filter_test WHERE has(m, '4') SETTINGS max_rows_to_read = 1, force_data_skipping_indices = 'idx_m';

SELECT count() FROM bloom_filter_test WHERE m['3'] = '3' SETTINGS max_rows_to_read = 1, force_data_skipping_indices = 'idx_m';
SELECT count() FROM bloom_filter_test WHERE m['4'] = '3' SETTINGS max_rows_to_read = 1, force_data_skipping_indices = 'idx_m';
SELECT count() FROM bloom_filter_test WHERE m['4'] = '';
SELECT count() FROM bloom_filter_test WHERE m['4'] != '';

SELECT count() FROM bloom_filter_test WHERE m['1'] > '0' SETTINGS max_rows_to_read = 1, force_data_skipping_indices = 'idx_m';
SELECT count() FROM bloom_filter_test WHERE m['1'] >= '0' SETTINGS max_rows_to_read = 1, force_data_skipping_indices = 'idx_m';
SELECT count() FROM bloom_filter_test WHERE m['1'] < '2' SETTINGS max_rows_to_read = 1, force_data_skipping_indices = 'idx_m';
SELECT count() FROM bloom_filter_test WHERE m['1'] <= '2' SETTINGS max_rows_to_read = 1, force_data_skipping_indices = 'idx_m';
SELECT count() FROM bloom_filter_test WHERE m['1'] like '1' SETTINGS max_rows_to_read = 1, force_data_skipping_indices = 'idx_m';
SELECT count() FROM bloom_filter_test WHERE m['4'] >= '';
SELECT count() FROM bloom_filter_test WHERE m['4'] <= '';


DROP TABLE IF EXISTS bloom_filter_token_test;

CREATE TABLE bloom_filter_token_test(id UInt64, a Nullable(String), m Map(String, String), INDEX idx_a a type tokenbf_v1(8192, 3, 0) GRANULARITY 1, INDEX idx_m m type tokenbf_v1(8192, 3, 0) GRANULARITY 1) ENGINE = MergeTree() ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO bloom_filter_token_test VALUES (1, '1,1', {'1,1': '1,1'}), (2, '2,2', {'2,2': '2,2'}), (3, NULL, {'3,3': '3,3'});

SELECT count() FROM bloom_filter_token_test WHERE a = '' SETTINGS force_data_skipping_indices = 'idx_a';
SELECT count() FROM bloom_filter_token_test WHERE a like '1,%' SETTINGS max_rows_to_read = 1, force_data_skipping_indices = 'idx_a';
SELECT count() FROM bloom_filter_token_test WHERE startsWith(a, '1,') SETTINGS max_rows_to_read = 1, force_data_skipping_indices = 'idx_a';
SELECT count() FROM bloom_filter_token_test WHERE hasToken(a, '2') SETTINGS max_rows_to_read = 1, force_data_skipping_indices = 'idx_a';

-- should disable new analyzer now because of some bugs
SELECT count() FROM bloom_filter_token_test WHERE mapContains(m, '1') SETTINGS max_rows_to_read = 1, force_data_skipping_indices = 'idx_m', enable_analyzer = 0;
SELECT count() FROM bloom_filter_token_test WHERE mapContains(m, '1,1') SETTINGS max_rows_to_read = 1, force_data_skipping_indices = 'idx_m', enable_analyzer = 0;
SELECT count() FROM bloom_filter_token_test WHERE mapContains(m, '4') SETTINGS max_rows_to_read = 1, force_data_skipping_indices = 'idx_m', enable_analyzer = 0;
SELECT count() FROM bloom_filter_token_test WHERE mapContainsKeyLike(m, '1') SETTINGS max_rows_to_read = 1, force_data_skipping_indices = 'idx_m', enable_analyzer = 0;
SELECT count() FROM bloom_filter_token_test WHERE mapContainsKeyLike(m, '1,1') SETTINGS max_rows_to_read = 1, force_data_skipping_indices = 'idx_m', enable_analyzer = 0;
SELECT count() FROM bloom_filter_token_test WHERE mapContainsKeyLike(m, '1,%') SETTINGS max_rows_to_read = 1, force_data_skipping_indices = 'idx_m', enable_analyzer = 0;
SELECT count() FROM bloom_filter_token_test WHERE mapContainsKeyLike(m, '4') SETTINGS max_rows_to_read = 1, force_data_skipping_indices = 'idx_m', enable_analyzer = 0;

SELECT count() FROM bloom_filter_token_test WHERE m['1'] = '1' SETTINGS max_rows_to_read = 1, force_data_skipping_indices = 'idx_m';
SELECT count() FROM bloom_filter_token_test WHERE m['1,1'] = '1,1' SETTINGS max_rows_to_read = 1, force_data_skipping_indices = 'idx_m';
SELECT count() FROM bloom_filter_token_test WHERE m['4'] = '0' SETTINGS max_rows_to_read = 1, force_data_skipping_indices = 'idx_m';


DROP TABLE IF EXISTS bloom_filter_ngram_test;

CREATE TABLE bloom_filter_ngram_test(id UInt64, a Nullable(String), m Map(String, String), INDEX idx_a a type ngrambf_v1(1, 8192, 3, 0) GRANULARITY 1, INDEX idx_m m type ngrambf_v1(1, 8192, 3, 0) GRANULARITY 1) ENGINE = MergeTree() ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO bloom_filter_ngram_test VALUES (1, '11', {'11': '11'}), (2, '22', {'22': '22'}), (3, NULL, {'33': '33'});

SELECT count() FROM bloom_filter_ngram_test WHERE a = '' SETTINGS force_data_skipping_indices = 'idx_a';
SELECT count() FROM bloom_filter_ngram_test WHERE a like '1%' SETTINGS max_rows_to_read = 1, force_data_skipping_indices = 'idx_a';
SELECT count() FROM bloom_filter_ngram_test WHERE startsWith(a, '1') SETTINGS max_rows_to_read = 1, force_data_skipping_indices = 'idx_a';
SELECT count() FROM bloom_filter_ngram_test WHERE hasToken(a, '2') SETTINGS max_rows_to_read = 1, force_data_skipping_indices = 'idx_a';

-- should disable new analyzer now because of some bugs
SELECT count() FROM bloom_filter_ngram_test WHERE mapContains(m, '1') SETTINGS max_rows_to_read = 1, force_data_skipping_indices = 'idx_m', enable_analyzer = 0;
SELECT count() FROM bloom_filter_ngram_test WHERE mapContains(m, '11') SETTINGS max_rows_to_read = 1, force_data_skipping_indices = 'idx_m', enable_analyzer = 0;
SELECT count() FROM bloom_filter_ngram_test WHERE mapContains(m, '4') SETTINGS max_rows_to_read = 1, force_data_skipping_indices = 'idx_m', enable_analyzer = 0;
SELECT count() FROM bloom_filter_ngram_test WHERE mapContainsKeyLike(m, '1') SETTINGS max_rows_to_read = 1, force_data_skipping_indices = 'idx_m', enable_analyzer = 0;
SELECT count() FROM bloom_filter_ngram_test WHERE mapContainsKeyLike(m, '11') SETTINGS max_rows_to_read = 1, force_data_skipping_indices = 'idx_m', enable_analyzer = 0;
SELECT count() FROM bloom_filter_ngram_test WHERE mapContainsKeyLike(m, '1%') SETTINGS max_rows_to_read = 1, force_data_skipping_indices = 'idx_m', enable_analyzer = 0;
SELECT count() FROM bloom_filter_ngram_test WHERE mapContainsKeyLike(m, '4') SETTINGS max_rows_to_read = 1, force_data_skipping_indices = 'idx_m', enable_analyzer = 0;

SELECT count() FROM bloom_filter_ngram_test WHERE m['1'] = '1' SETTINGS max_rows_to_read = 1, force_data_skipping_indices = 'idx_m';
SELECT count() FROM bloom_filter_ngram_test WHERE m['11'] = '11' SETTINGS max_rows_to_read = 1, force_data_skipping_indices = 'idx_m';
SELECT count() FROM bloom_filter_ngram_test WHERE m['4'] = '0' SETTINGS max_rows_to_read = 1, force_data_skipping_indices = 'idx_m';
