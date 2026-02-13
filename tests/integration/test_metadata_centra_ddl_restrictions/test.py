"""
Integration tests for DDL restrictions when metadata centralization is enabled.

Tests cover restrictions on:
- Database DDL: ON CLUSTER, RENAME, DETACH
- Table DDL: ON CLUSTER, RENAME, DETACH, DROP/ADD INDEX/COLUMN/PROJECTION

And allowed operations:
- ALTER TABLE ATTACH PARTITION FROM
- ALTER TABLE DROP PART
- ALTER TABLE DROP PARTITION
"""
import logging
import time
import pytest
from typing import List, Dict

from helpers.client import QueryRuntimeException
from helpers.cluster import ClickHouseCluster

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class MetricsHelper:
    """Helper class for metrics verification."""

    @staticmethod
    def wait_for_sync(timeout: float = 3.0) -> None:
        """Wait for metadata synchronization."""
        time.sleep(timeout)
        logger.debug(f"Waited {timeout}s for metadata sync")
    
    @staticmethod
    def verify_metrics_consistency(cluster, node_names: List[str], expected_values: Dict[str, int]) -> None:
        """Verify metrics consistency across multiple nodes."""
        for node_name in node_names:
            node = cluster.instances[node_name]
            MetricsHelper.verify_metrics(node, expected_values, node_name)

    @staticmethod
    def get_metrics(node, metric_names: List[str]) -> Dict[str, int]:
        """Get metrics values from system.metrics."""
        metrics = {}
        for metric_name in metric_names:
            query = f"SELECT value FROM system.metrics WHERE metric = '{metric_name}'"
            result = node.query(query).strip()
            metrics[metric_name] = int(result) if result else 0
        return metrics

    @staticmethod
    def verify_metrics(node, expected_values: Dict[str, int], node_name: str = None) -> None:
        """Verify metrics values on a single node."""
        if node_name is None:
            node_name = node.name

        actual_metrics = MetricsHelper.get_metrics(node, list(expected_values.keys()))

        for metric_name, expected_value in expected_values.items():
            actual_value = actual_metrics.get(metric_name, -1)
            assert actual_value == expected_value, (
                f"Metric {metric_name} mismatch on {node_name}: "
                f"expected {expected_value}, got {actual_value}"
            )
            logger.info(f"✓ {node_name}: {metric_name} = {actual_value}")
    @staticmethod
    def wait_and_verify_metrics(node, expected_values: Dict[str, int],
                                max_retries: int = 5, retry_interval: float = 1.0,
                                node_name: str = None) -> None:
        """Wait and verify metrics with retries."""
        if node_name is None:
            node_name = node.name

        for attempt in range(1, max_retries + 1):
            try:
                MetricsHelper.verify_metrics(node, expected_values, node_name)
                logger.info(f"✓ Metrics verified on {node_name} (attempt {attempt}/{max_retries})")
                return
            except AssertionError as e:
                if attempt == max_retries:
                    logger.error(f"✗ Metrics verification failed on {node_name} after {max_retries} attempts")
                    raise
                logger.warning(f"Metrics not ready on {node_name}, retrying... (attempt {attempt}/{max_retries})")
                time.sleep(retry_interval)


@pytest.fixture(scope="module")
def cluster():
    """Fixture to create and manage ClickHouse cluster for testing."""
    cluster = ClickHouseCluster(__file__)

    # Configuration for 2 shards with 1 replicas each
    # node1 uses special config to initialize Boss manifest
    cluster.add_instance(
        "node1",
        main_configs=[
            "configs/config.d/metadata_centra_node1.xml",
            "configs/config.d/storage_conf.xml",
        ],
        macros={"replica": "replica1", "shard": "shard1", "layer": "test"},
        with_minio=True,
        with_zookeeper=True,
    )

    # node2 use default config (initialize_centralized_metadata=false)
    cluster.add_instance(
        "node2",
        main_configs=[
            "configs/config.d/metadata_centra.xml",
            "configs/config.d/storage_conf.xml",
        ],
        macros={"replica": "replica1", "shard": "shard2", "layer": "test"},
        with_minio=True,
        with_zookeeper=True,
    )

    try:
        logger.info("Starting cluster...")

        cluster.start()

        node1 = cluster.instances["node1"]
        node2 = cluster.instances["node2"]

        # Wait for node1 to initialize
        logger.info("Waiting for node1 to initialize Boss manifest...")
        MetricsHelper.wait_for_sync()

        # Verify node1 metrics after initialization
        logger.info("Verifying node1 initial metrics...")
        expected_initial_metrics = {
            "MetaCentraBossManifestVersion": 1,
            "MetaCentraBossDatabaseCount": 0,
            "MetaCentraBossTableCount": 0,
            "MetaCentraLocalManifestVersion": 1,
            "MetaCentraLocalDatabaseCount": 0,
            "MetaCentraLocalTableCount": 0,
            "MetaCentraSyncFromBossStatus": 0,
        }
        MetricsHelper.verify_metrics(node1, expected_initial_metrics, "node1")

        logger.info("Cluster initialization and metrics verification completed")

        yield cluster
    finally:
        MetricsHelper.wait_for_sync()
        logger.info("in final state...")
        expected_in_final_state = {
            "MetaCentraBossManifestVersion": 69,
            "MetaCentraBossDatabaseCount": 0,
            "MetaCentraBossTableCount": 0,
            "MetaCentraLocalManifestVersion": 69,
            "MetaCentraLocalDatabaseCount": 0,
            "MetaCentraLocalTableCount": 0,
            "MetaCentraSyncFromBossStatus": 0,
        }
        MetricsHelper.verify_metrics_consistency(cluster, ["node1", "node2"], expected_in_final_state)

        cluster.shutdown()


@pytest.fixture(autouse=True)
def setup_test_env(cluster):
    """Setup and cleanup for each test."""
    node1 = cluster.instances["node1"]
    node2 = cluster.instances["node2"]

    # Create test database and tables
    node1.query("CREATE DATABASE IF NOT EXISTS test_db_v1")

    # Create test tables with INDEX and PROJECTION
    node2.query("""
        CREATE TABLE IF NOT EXISTS test_db_v1.test_tb_v1
        (
            id UInt64,
            dt DateTime,
            name String,
            city String,
            INDEX idx1 name TYPE bloom_filter GRANULARITY 1,
            PROJECTION agg_p1 (SELECT city, count(), sum(id) GROUP BY city)
        )
        ENGINE = MergeTree
        PARTITION BY toDate(dt)
        ORDER BY id
        TTL dt + toIntervalDay(7)
        SETTINGS storage_policy = 'hot_and_cold'
    """)

    node1.query("""
        CREATE TABLE IF NOT EXISTS test_db_v1.test_tb_v2
        (
            id UInt64,
            dt DateTime,
            name String,
            city String,
            INDEX idx1 name TYPE bloom_filter GRANULARITY 1,
            PROJECTION agg_p1 (SELECT city, count(), sum(id) GROUP BY city)
        )
        ENGINE = MergeTree
        PARTITION BY toDate(dt)
        ORDER BY id
        TTL dt + toIntervalDay(7)
        SETTINGS storage_policy = 'hot_and_cold'
    """)

    yield

    # Cleanup
    logger.info("Running cleanup after test")
    try:
        node1.query("DROP DATABASE IF EXISTS test_db_v1 SYNC")
    except Exception as e:
        logger.warning(f"Error during cleanup: {e}")

    time.sleep(2)


# ==================== Database DDL Restriction Tests ====================

def test_create_database_on_cluster_fails(cluster):
    """Test that CREATE DATABASE ON CLUSTER is not supported."""
    node1 = cluster.instances["node1"]

    with pytest.raises(QueryRuntimeException) as exc_info:
        node1.query("CREATE DATABASE test_db_on_cluster ON CLUSTER test_meta_cetra")

    assert "INCORRECT_QUERY" in str(exc_info.value) and "Code: 80" in str(exc_info.value)
    assert "Distributed DDL operations are not supported when metadata centralization is enabled" in str(exc_info.value)
    logger.info("CREATE DATABASE ON CLUSTER correctly rejected")


def test_rename_database_fails(cluster):
    """Test that RENAME DATABASE is not supported."""
    node2 = cluster.instances["node2"]

    with pytest.raises(QueryRuntimeException) as exc_info:
        node2.query("RENAME DATABASE test_db_v1 TO test_db_v1_rv1")

    assert "NOT_IMPLEMENTED" in str(exc_info.value) and "Code: 48" in str(exc_info.value)
    assert "RENAME operation is not" in str(exc_info.value) and "metadata centralization" in str(exc_info.value)
    logger.info("RENAME DATABASE correctly rejected")


def test_detach_database_fails(cluster):
    """Test that DETACH DATABASE is not supported."""
    node1 = cluster.instances["node1"]

    with pytest.raises(QueryRuntimeException) as exc_info:
        node1.query("DETACH DATABASE test_db_v1")

    assert "NOT_IMPLEMENTED" in str(exc_info.value) and "Code: 48" in str(exc_info.value)
    assert "DETACH operation is not implemented for metadata centralization" in str(exc_info.value)
    logger.info("DETACH DATABASE correctly rejected")


def test_drop_database_on_cluster_fails(cluster):
    """Test that DROP DATABASE ON CLUSTER is not supported."""
    node1 = cluster.instances["node1"]

    with pytest.raises(QueryRuntimeException) as exc_info:
        node1.query("DROP DATABASE test_db_v1 ON CLUSTER test_meta_cetra")

    assert "INCORRECT_QUERY" in str(exc_info.value) and "Code: 80" in str(exc_info.value)
    assert "Distributed DDL operations are not supported when metadata centralization is enabled" in str(exc_info.value)
    logger.info("DROP DATABASE ON CLUSTER correctly rejected")


# ==================== Table DDL Restriction Tests ====================

def test_create_table_on_cluster_fails(cluster):
    """Test that CREATE TABLE ON CLUSTER is not supported."""
    node1 = cluster.instances["node1"]

    with pytest.raises(QueryRuntimeException) as exc_info:
        node1.query("""
            CREATE TABLE test_db_v1.test_tb_on_cluster ON CLUSTER test_meta_cetra
            (
                id UInt64,
                dt DateTime
            )
            ENGINE = MergeTree
            PARTITION BY toDate(dt)
            ORDER BY id
            TTL dt + toIntervalDay(7)
            SETTINGS storage_policy = 'hot_and_cold', index_granularity = 8192
        """)

    assert "INCORRECT_QUERY" in str(exc_info.value) and "Code: 80" in str(exc_info.value)
    assert "Distributed DDL operations are not supported when metadata centralization is enabled" in str(exc_info.value)
    logger.info("CREATE TABLE ON CLUSTER correctly rejected")


def test_rename_table_fails(cluster):
    """Test that RENAME TABLE is not supported."""
    node1 = cluster.instances["node1"]

    with pytest.raises(QueryRuntimeException) as exc_info:
        node1.query("RENAME TABLE test_db_v1.test_tb_v1 TO test_db_v1.test_tb_v1_renamed")

    assert "NOT_IMPLEMENTED" in str(exc_info.value) and "Code: 48" in str(exc_info.value)
    assert "RENAME operation is not" in str(exc_info.value) and "metadata centralization" in str(exc_info.value)
    logger.info("RENAME TABLE correctly rejected")


def test_detach_table_fails(cluster):
    """Test that DETACH TABLE is not supported."""
    node1 = cluster.instances["node1"]

    with pytest.raises(QueryRuntimeException) as exc_info:
        node1.query("DETACH TABLE test_db_v1.test_tb_v1")

    assert "NOT_IMPLEMENTED" in str(exc_info.value) and "Code: 48" in str(exc_info.value)
    assert "DETACH operation is not implemented for metadata centralization" in str(exc_info.value)
    logger.info("DETACH TABLE correctly rejected")


def test_drop_table_on_cluster_fails(cluster):
    """Test that DROP TABLE ON CLUSTER is not supported."""
    node1 = cluster.instances["node1"]

    with pytest.raises(QueryRuntimeException) as exc_info:
        node1.query("DROP TABLE test_db_v1.test_tb_v1 ON CLUSTER test_meta_cetra")

    assert "INCORRECT_QUERY" in str(exc_info.value) and "Code: 80" in str(exc_info.value)
    assert "Distributed DDL operations are not supported when metadata centralization is enabled" in str(exc_info.value)
    logger.info("DROP TABLE ON CLUSTER correctly rejected")


def test_alter_table_on_cluster_fails(cluster):
    """Test that ALTER TABLE ON CLUSTER is not supported."""
    node1 = cluster.instances["node1"]

    with pytest.raises(QueryRuntimeException) as exc_info:
        node1.query("ALTER TABLE test_db_v1.test_tb_v1 ON CLUSTER test_meta_cetra ADD COLUMN new_col String")

    assert "INCORRECT_QUERY" in str(exc_info.value) and "Code: 80" in str(exc_info.value)
    assert "Distributed DDL operations are not supported when metadata centralization is enabled" in str(exc_info.value)
    logger.info("ALTER TABLE ON CLUSTER correctly rejected")


def test_alter_table_drop_index_fails(cluster):
    """Test that ALTER TABLE DROP INDEX is not supported."""
    node1 = cluster.instances["node1"]

    with pytest.raises(QueryRuntimeException) as exc_info:
        node1.query("ALTER TABLE test_db_v1.test_tb_v1 DROP INDEX idx1")

    assert "NOT_IMPLEMENTED" in str(exc_info.value) and "Code: 48" in str(exc_info.value)
    assert "DROP_INDEX" in str(exc_info.value)
    assert "Only ADD COLUMN, ADD INDEX, ADD PROJECTION, MODIFY SETTING and MODIFY TTL operations are supported" in str(exc_info.value)
    logger.info("ALTER TABLE DROP INDEX correctly rejected")


def test_alter_table_drop_column_fails(cluster):
    """Test that ALTER TABLE DROP COLUMN is not supported."""
    node2 = cluster.instances["node2"]

    with pytest.raises(QueryRuntimeException) as exc_info:
        node2.query("ALTER TABLE test_db_v1.test_tb_v1 DROP COLUMN city")

    assert "NOT_IMPLEMENTED" in str(exc_info.value) and "Code: 48" in str(exc_info.value)
    assert "DROP_COLUMN" in str(exc_info.value)
    assert "Only ADD COLUMN, ADD INDEX, ADD PROJECTION, MODIFY SETTING and MODIFY TTL operations are supported" in str(exc_info.value)
    logger.info("ALTER TABLE DROP COLUMN correctly rejected")


def test_alter_table_drop_projection_fails(cluster):
    """Test that ALTER TABLE DROP PROJECTION is not supported."""
    node1 = cluster.instances["node1"]

    with pytest.raises(QueryRuntimeException) as exc_info:
        node1.query("ALTER TABLE test_db_v1.test_tb_v1 DROP PROJECTION agg_p1")

    assert "NOT_IMPLEMENTED" in str(exc_info.value) and "Code: 48" in str(exc_info.value)
    assert "DROP_PROJECTION" in str(exc_info.value)
    assert "Only ADD COLUMN, ADD INDEX, ADD PROJECTION, MODIFY SETTING and MODIFY TTL operations are supported" in str(exc_info.value)
    logger.info("ALTER TABLE DROP PROJECTION correctly rejected")

def test_alter_table_modify_ttl_with_mutation_fails(cluster):
    """Test that ALTER TABLE DROP PROJECTION is not supported."""
    node1 = cluster.instances["node1"]

    # materialize_ttl_after_modify default value is 1
    with pytest.raises(QueryRuntimeException) as exc_info:
        node1.query(
        "ALTER TABLE test_db_v1.test_tb_v1_local MODIFY TTL dt + toIntervalDay(15) settings materialize_ttl_after_modify = 1"
    )

    assert "INCORRECT_QUERY" in str(exc_info.value) and "Code: 80" in str(exc_info.value)
    assert "MODIFY TTL" in str(exc_info.value)
    assert "MODIFY TTL operations that trigger mutations are not supported when metadata centralization is enabled" in str(exc_info.value)
    logger.info("ALTER TABLE MODIFY TTL WITH MUTATION correctly rejected")

def test_create_lazy_database_fails(cluster):
    """Test that CREATE Lazy DATABASE is not supported."""
    node1 = cluster.instances["node1"]

    with pytest.raises(QueryRuntimeException) as exc_info:
        node1.query("CREATE DATABASE testlazy ENGINE = Lazy(5)")

    assert "NOT_IMPLEMENTED" in str(exc_info.value) and "Code: 48" in str(exc_info.value)
    assert "Database engine 'Lazy' is not supported when metadata centralization is enabled. Only the Atomic engine is supported" in str(exc_info.value)
    logger.info("CREATE Lazy DATABASE correctly rejected")

def test_create_materialized_view_fails(cluster):
    """Test that CREATE MATERIALIZED VIEW is not supported."""
    node1 = cluster.instances["node1"]

    with pytest.raises(QueryRuntimeException) as exc_info:
        node1.query("CREATE MATERIALIZED VIEW IF NOT EXISTS test_db_v1.test_tb_v1_mv ENGINE = SummingMergeTree PARTITION BY dt ORDER BY name SETTINGS index_granularity = 8192, storage_policy = 'hot_and_cold' AS SELECT toDate(dt) as dt, name, count() AS visit, sum(id) AS sum_id FROM test_db_v1.test_tb_v1 GROUP BY dt,name")

    assert "NOT_IMPLEMENTED" in str(exc_info.value) and "Code: 48" in str(exc_info.value)
    assert "Materialized views are not supported with metadata centralization" in str(exc_info.value)
    logger.info("CREATE MATERIALIZED VIEW correctly rejected")

def test_create_dictionary_fails(cluster):
    """Test that CREATE DICTIONARY is not supported."""
    
    node1 = cluster.instances["node1"]

    with pytest.raises(QueryRuntimeException) as exc_info:
        node1.query("CREATE DICTIONARY test_db_v1.dict_app_id (`app_id` Int64 DEFAULT 0, `app_name` String DEFAULT '') PRIMARY KEY app_id SOURCE(MYSQL(PORT 3802 USER 'test_user' PASSWORD 'xxxxx' REPLICA (HOST 'xxx.com.co' PRIORITY 1) DB 'db_v1' TABLE 'table_v1' INVALIDATE_QUERY 'select max(mtime)')) LIFETIME(MIN 3600 MAX 4000) LAYOUT(HASHED())")

    assert "NOT_IMPLEMENTED" in str(exc_info.value) and "Code: 48" in str(exc_info.value)
    assert "CREATE TABLE queries without an explicit column list or CREATE TABLE AS are not supported when metadata centralization is enabled" in str(exc_info.value)
    logger.info("CREATE TABLE DICTIONARY correctly rejected")

# ==================== Allowed Operations Tests ====================

def test_allowed_data_operations(cluster):
    """
    Test allowed data operations in sequence to avoid data corruption in multi-threaded execution.

    Tests in order:
    1. SELECT from table
    2. ALTER TABLE ATTACH PARTITION FROM
    3. ALTER TABLE DROP PART
    4. ALTER TABLE DROP PARTITION
    """
    node1 = cluster.instances["node1"]

    # ========== Step 1: Test SELECT from table ==========
    logger.info("Step 1: Testing SELECT from table")

    # Insert data
    node1.query("INSERT INTO test_db_v1.test_tb_v1 VALUES (1, now(), 'a', 'sh')")
    node1.query("INSERT INTO test_db_v1.test_tb_v1 VALUES (2, now(), 'b', 'bj')")

    result = node1.query("SELECT id, name, city FROM test_db_v1.test_tb_v1 ORDER BY id")
    assert "sh" in result or "bj" in result, f"Expected data in result, got: {result}"
    
    # Verify data is in test_tb_v1
    count_v1 = node1.query("SELECT count() FROM test_db_v1.test_tb_v1").strip()
    assert int(count_v1) == 2, f"Expected data in count_v1, got {count_v1} rows"

    # ========== Step 2: Test ALTER TABLE ATTACH PARTITION FROM ==========
    logger.info("Step 2: Testing ALTER TABLE ATTACH PARTITION FROM")

    # Get partition name from test_tb_v1
    partition = node1.query(
        "SELECT partition FROM system.parts WHERE table = 'test_tb_v1' AND database = 'test_db_v1' AND active = 1 LIMIT 1"
    ).strip()

    assert partition, "No partition found for ATTACH PARTITION test"
    logger.info(f"Found partition for attach: {partition}")

    # ATTACH PARTITION FROM should succeed
    node1.query(f"ALTER TABLE test_db_v1.test_tb_v2 ATTACH PARTITION '{partition}' FROM test_db_v1.test_tb_v1")

    # Verify data is in test_tb_v2
    count_v2 = node1.query("SELECT count() FROM test_db_v1.test_tb_v2").strip()
    assert int(count_v2) == 2, f"Expected data in test_tb_v2, got {count_v2} rows"
    logger.info("ALTER TABLE ATTACH PARTITION FROM executed successfully")

    # ========== Step 3: Test ALTER TABLE DROP PART ==========
    logger.info("Step 3: Testing ALTER TABLE DROP PART")

    # Insert more data for drop part test
    node1.query("INSERT INTO test_db_v1.test_tb_v1 VALUES (3, now(), 'c', 'gz')")

    # Verify data is in test_tb_v1
    count_v1 = node1.query("SELECT count() FROM test_db_v1.test_tb_v1").strip()
    assert int(count_v1) == 3, f"Expected data in count_v1, got {count_v1} rows"

    # Get part name
    part_name = node1.query(
        "SELECT name FROM system.parts WHERE table = 'test_tb_v1' AND database = 'test_db_v1' AND active = 1 LIMIT 1"
    ).strip()

    assert part_name, "No part found for DROP PART test"
    logger.info(f"Found part: {part_name}")

    # DROP PART should succeed
    node1.query(f"ALTER TABLE test_db_v1.test_tb_v1 DROP PART '{part_name}'")

    # Verify data is in test_tb_v1
    count_v1 = node1.query("SELECT count() FROM test_db_v1.test_tb_v1").strip()
    assert int(count_v1) == 2, f"Expected data in count_v1, got {count_v1} rows"

    logger.info("ALTER TABLE DROP PART executed successfully")

    # ========== Step 4: Test ALTER TABLE DROP PARTITION ==========
    logger.info("Step 4: Testing ALTER TABLE DROP PARTITION")

    # Insert more data for drop partition test
    node1.query("INSERT INTO test_db_v1.test_tb_v1 VALUES (4, now(), 'd', 'sz')")

    # Verify data is in test_tb_v1
    count_v1 = node1.query("SELECT count() FROM test_db_v1.test_tb_v1").strip()
    assert int(count_v1) == 3, f"Expected data in count_v1, got {count_v1} rows"

    # Get partition name
    partition = node1.query(
        "SELECT partition FROM system.parts WHERE table = 'test_tb_v1' AND database = 'test_db_v1' AND active = 1 LIMIT 1"
    ).strip()

    assert partition, "No partition found for DROP PARTITION test"
    logger.info(f"Found partition: {partition}")

    # DROP PARTITION should succeed
    node1.query(f"ALTER TABLE test_db_v1.test_tb_v1 DROP PARTITION '{partition}'")

    # Verify data is in test_tb_v1
    count_v1 = node1.query("SELECT count() FROM test_db_v1.test_tb_v1").strip()
    assert int(count_v1) == 0, f"Expected data in count_v1, got {count_v1} rows"
    logger.info("ALTER TABLE DROP PARTITION executed successfully")

    logger.info("All allowed data operations completed successfully")
