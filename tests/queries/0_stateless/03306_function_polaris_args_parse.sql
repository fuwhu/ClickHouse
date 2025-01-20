DROP TABLE IF EXISTS polaris_args_parse_test;

CREATE TABLE polaris_args_parse_test
(
    `id` UInt64,
    `fields_keys` Array(String),
    `fields_values` Map(String, Array(UInt64))
) 
ENGINE = MergeTree() 
ORDER BY id;

INSERT INTO polaris_args_parse_test values (1, ['k1', 'k2', 'k3'], {'sss`2`3.3': [1, 1]}), (2, ['k2', 'k3', 'k4'], {'2`3.3`-4': [2, 2]}), (3, ['k1', 'k4'], {'sss`-4': [3, 3]});

SELECT id, polarisArgsParse(fields_keys, fields_values, 'and', ('k1', '=', 'sss'), ('k2', '=', '2')) FROM polaris_args_parse_test;
SELECT id, polarisArgsParse(fields_keys, fields_values, 'and', ('k1', '=', 'sss'), ('k4', '=', '-4')) FROM polaris_args_parse_test;
SELECT id, polarisArgsParse(fields_keys, fields_values, 'and', ('k1', '=', 'sss'), ('k2', '!=', '2')) FROM polaris_args_parse_test;
SELECT id, polarisArgsParse(fields_keys, fields_values, 'and', ('k1', '!=', 'sss'), ('k4', '=', '-4')) FROM polaris_args_parse_test;

SELECT id, polarisArgsParse(fields_keys, fields_values, 'or', ('k1', '=', 'sss'), ('k2', '=', '2')) FROM polaris_args_parse_test;
SELECT id, polarisArgsParse(fields_keys, fields_values, 'or', ('k1', '=', 'sss'), ('k4', '=', '-4')) FROM polaris_args_parse_test;
SELECT id, polarisArgsParse(fields_keys, fields_values, 'or', ('k1', '=', 'sss'), ('k2', '!=', '2')) FROM polaris_args_parse_test;
SELECT id, polarisArgsParse(fields_keys, fields_values, 'or', ('k1', '!=', 'sss'), ('k4', '=', '-4')) FROM polaris_args_parse_test;

SELECT id, polarisArgsParse(fields_keys, fields_values, 'and', ('k2', '>', '2'), ('k3', '>', '2.2'), ('k4', '>=', '-4')) FROM polaris_args_parse_test;
SELECT id, polarisArgsParse(fields_keys, fields_values, 'and', ('k2', '<=', '2'), ('k3', '<', '2.2'), ('k4', '<', '-4')) FROM polaris_args_parse_test;
SELECT id, polarisArgsParse(fields_keys, fields_values, 'and', ('k2', '>', '1'), ('k3', '>=', '2.2'), ('k4', '<=', '0')) FROM polaris_args_parse_test;
SELECT id, polarisArgsParse(fields_keys, fields_values, 'and', ('k2', '>=', '1'), ('k3', '<=', '2.2'), ('k4', '<=', '-1')) FROM polaris_args_parse_test;

SELECT id, polarisArgsParse(fields_keys, fields_values, 'or', ('k2', '>', '2'), ('k3', '>', '2.2'), ('k4', '>=', '-4')) FROM polaris_args_parse_test;
SELECT id, polarisArgsParse(fields_keys, fields_values, 'or', ('k2', '<=', '2'), ('k3', '<', '2.2'), ('k4', '<', '-4')) FROM polaris_args_parse_test;
SELECT id, polarisArgsParse(fields_keys, fields_values, 'or', ('k2', '>', '1'), ('k3', '>=', '2.2'), ('k4', '<=', '0')) FROM polaris_args_parse_test;
SELECT id, polarisArgsParse(fields_keys, fields_values, 'or', ('k2', '>=', '1'), ('k3', '<=', '2.2'), ('k4', '<=', '-1')) FROM polaris_args_parse_test;

TRUNCATE TABLE polaris_args_parse_test;

INSERT INTO polaris_args_parse_test values (1, ['k1', 'k2', 'k3'], {'k1`k2`k3': [1, 1]}), (2, ['k2', 'k3', 'k4'], {'k2`k3`k4': [2, 2]}), (3, ['k1', 'k4'], {'k1`k4': [3, 3]});

SELECT id, polarisArgsParse(fields_keys, fields_values, 'and', ('k1', 'in', 'k1,k2'), ('k2', 'in', 'k2,k3')) FROM polaris_args_parse_test;
SELECT id, polarisArgsParse(fields_keys, fields_values, 'and', ('k1', 'in', 'k1,k2'), ('k2', 'not in', 'k2,k3')) FROM polaris_args_parse_test;
SELECT id, polarisArgsParse(fields_keys, fields_values, 'and', ('k1', 'not in', 'k1,k2'), ('k2', 'in', 'k2,k3')) FROM polaris_args_parse_test;
SELECT id, polarisArgsParse(fields_keys, fields_values, 'and', ('k1', 'not in', 'k1,k2'), ('k2', 'not in', 'k2,k3')) FROM polaris_args_parse_test;

SELECT id, polarisArgsParse(fields_keys, fields_values, 'or', ('k1', 'in', 'k1,k2'), ('k2', 'in', 'k2,k3')) FROM polaris_args_parse_test;
SELECT id, polarisArgsParse(fields_keys, fields_values, 'or', ('k1', 'in', 'k1,k2'), ('k2', 'not in', 'k2,k3')) FROM polaris_args_parse_test;
SELECT id, polarisArgsParse(fields_keys, fields_values, 'or', ('k1', 'not in', 'k1,k2'), ('k2', 'in', 'k2,k3')) FROM polaris_args_parse_test;
SELECT id, polarisArgsParse(fields_keys, fields_values, 'or', ('k1', 'not in', 'k1,k2'), ('k2', 'not in', 'k2,k3')) FROM polaris_args_parse_test;

SELECT id, polarisArgsParse(fields_keys, fields_values, 'and', ('k1', 'like', 'k1.*'), ('k2', 'like', 'k2.*')) FROM polaris_args_parse_test;
SELECT id, polarisArgsParse(fields_keys, fields_values, 'and', ('k1', 'like', 'k2.*'), ('k2', 'not like', 'k3.*')) FROM polaris_args_parse_test;
SELECT id, polarisArgsParse(fields_keys, fields_values, 'and', ('k1', 'not like', 'k1.*'), ('k2', 'like', 'k2.*')) FROM polaris_args_parse_test;
SELECT id, polarisArgsParse(fields_keys, fields_values, 'and', ('k1', 'not like', 'k2.*'), ('k2', 'not like', 'k3.*')) FROM polaris_args_parse_test;

SELECT id, polarisArgsParse(fields_keys, fields_values, 'or', ('k1', 'like', 'k1.*'), ('k2', 'like', 'k2.*')) FROM polaris_args_parse_test;
SELECT id, polarisArgsParse(fields_keys, fields_values, 'or', ('k1', 'like', 'k2.*'), ('k2', 'not like', 'k3.*')) FROM polaris_args_parse_test;
SELECT id, polarisArgsParse(fields_keys, fields_values, 'or', ('k1', 'not like', 'k1.*'), ('k2', 'like', 'k2.*')) FROM polaris_args_parse_test;
SELECT id, polarisArgsParse(fields_keys, fields_values, 'or', ('k1', 'not like', 'k2.*'), ('k2', 'not like', 'k3.*')) FROM polaris_args_parse_test;
