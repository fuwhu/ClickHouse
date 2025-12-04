DROP TABLE IF EXISTS metric_app_test;

CREATE TABLE metric_app_test
(
    `__timestamp__` Nullable(Int64),
    `__value__` Nullable(Decimal(38, 1)),
    `__value_v2__` Nullable(Decimal(38, 1)),
    `le` Nullable(String)
)
ENGINE = MergeTree
ORDER BY tuple();

INSERT INTO metric_app_test VALUES (1739274165884, 5356710, null, '500'), (1739274165885, 5353834, 50, '30'), (1739274165886, null, null, '25'), (1739274165887, 1503, 3000, null), (1739274165888, 7, 10000, '100'), (1739274165889, 7, 10, '+INF'), (1739274165890, 5356715, null, '+Inf');

SELECT metric_increase(120000)(__timestamp__, __value__) AS __value__ FROM metric_app_test;
SELECT metric_histogram_quantile(le, __value__) FROM metric_app_test;
SELECT metric_histogram_quantile(0)(le, __value__) FROM metric_app_test;
SELECT metric_histogram_quantile(0.5)(le, __value__) FROM metric_app_test;
SELECT metric_histogram_quantile(0.9)(le, __value__) FROM metric_app_test;
SELECT metric_histogram_quantile(0.99)(le, __value__) FROM metric_app_test;
SELECT metric_histogram_quantile(1)(le, __value__) FROM metric_app_test;
SELECT metric_rate(__timestamp__, __value__) AS __value__ FROM metric_app_test;
SELECT metric_irate(__timestamp__, __value__) AS __value__ FROM metric_app_test;

DROP TABLE IF EXISTS metric_app_test;


DROP TABLE IF EXISTS metric_app_test_not_null;

CREATE TABLE metric_app_test_not_null
(
    `__timestamp__` Int64,
    `__value__` Decimal(38, 1),
    `__value_v2__` Decimal(38, 1),
    `le` String
)
ENGINE = MergeTree
ORDER BY tuple();

INSERT INTO metric_app_test_not_null VALUES (1739274165884, 5356710, 1000, '500'), (1739274165885, 5353834, 50, '30'), (1739274165886, 5353835, 20, '25'), (1739274165887, 1503, 3000, '24.5'), (1739274165888, 7, 10000, '100'), (1739274165889, 7, 10, '+INF'), (1739274165890, 5356715, 1000, '+Inf');

SELECT metric_increase(120000)(__timestamp__, __value__) AS __value__ FROM metric_app_test_not_null;
SELECT metric_histogram_quantile(le, __value__) FROM metric_app_test_not_null;
SELECT metric_histogram_quantile(0)(le, __value__) FROM metric_app_test_not_null;
SELECT metric_histogram_quantile(0.5)(le, __value__) FROM metric_app_test_not_null;
SELECT metric_histogram_quantile(0.9)(le, __value__) FROM metric_app_test_not_null;
SELECT metric_histogram_quantile(0.99)(le, __value__) FROM metric_app_test_not_null;
SELECT metric_histogram_quantile(1)(le, __value__) FROM metric_app_test_not_null;
SELECT metric_rate(__timestamp__, __value__) AS __value__ FROM metric_app_test_not_null;
SELECT metric_irate(__timestamp__, __value__) AS __value__ FROM metric_app_test_not_null;

DROP TABLE IF EXISTS metric_app_test_not_null;
