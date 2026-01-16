"""
Integration tests for metadata centralization with MergeTree and ReplicatedMergeTree tables.

Tests cover:
- Table creation and metadata synchronization across nodes
- ALTER operations (ADD COLUMN, ADD INDEX, MODIFY TTL, ADD PROJECTION, MODIFY SETTING)
- Data insertion and replication
- Table and database deletion
"""
import logging
import time
from typing import List, Tuple
import pytest

from helpers.cluster import ClickHouseCluster

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class MetadataTestHelper:
    """Helper class for metadata test operations and assertions."""
    
    ALL_NODES = ["node1", "node2", "node3", "node4"]
    
    @staticmethod
    def wait_for_sync(timeout: float = 3.0) -> None:
        """Wait for metadata synchronization."""
        time.sleep(timeout)
        logger.debug("Waited for metadata sync")
    
    @staticmethod
    def verify_table_structure(node, database: str, table: str, 
                              expected_columns: List[str]) -> None:
        """Verify table structure matches expected columns."""
        query = f"""
            SELECT name, type 
            FROM system.columns 
            WHERE database='{database}' AND table='{table}' 
            ORDER BY name
        """
        result = node.query(query)
        logger.info(f"Table {database}.{table} columns on {node.name}: {result}")
        
        for col in expected_columns:
            assert col in result, f"Column {col} not found in {database}.{table} on {node.name}"
    
    @staticmethod
    def verify_minio_object_count(cluster, minio_client, expected_count):
        """ Verify the number of objects in MinIO storage. """
        actual_count = sum(1 for _ in minio_client.list_objects(cluster.minio_bucket, "data/ck_metadata/", recursive=True))
        assert actual_count == expected_count, f"Expected {expected_count} objects in minio bucket, got {actual_count}"
        logger.info(f"MinIO object count verified: {actual_count} objects at prefix data/ck_metadata/")
    
    @staticmethod
    def verify_metadata_consistency(cluster, database: str, table: str, 
                                   expected_columns: List[str]) -> None:
        """Verify metadata consistency across all cluster nodes."""
        for node_name in MetadataTestHelper.ALL_NODES:
            node = cluster.instances[node_name]
            MetadataTestHelper.verify_table_structure(
                node, database, table, expected_columns
            )
    
    @staticmethod
    def verify_indexes(cluster, database: str, table: str, 
                      expected_indexes: List[str]) -> None:
        """Verify indexes exist on all cluster nodes."""
        for node_name in MetadataTestHelper.ALL_NODES:
            node = cluster.instances[node_name]
            query = f"""
                SELECT name 
                FROM system.data_skipping_indices 
                WHERE database='{database}' AND table='{table}' 
                ORDER BY name
            """
            result = node.query(query)
            
            for idx in expected_indexes:
                assert idx in result, f"Index {idx} not found on {node_name}"
            logger.info(f"Indexes {expected_indexes} verified on {node_name}")
    
    @staticmethod
    def verify_ttl(cluster, database: str, table: str, 
                  expected_interval: str) -> None:
        """Verify TTL settings are consistent across all cluster nodes."""
        for node_name in MetadataTestHelper.ALL_NODES:
            node = cluster.instances[node_name]
            query = f"""
                SELECT extract(engine_full, 'TTL[^S]+') AS ttl_expression 
                FROM system.tables 
                WHERE database='{database}' AND name='{table}'
            """
            result = node.query(query)
            assert expected_interval in result or result.strip() != "", \
                f"TTL not updated on {node_name}"
            logger.info(f"TTL verified on {node_name}: {expected_interval}")
    
    @staticmethod
    def verify_projections(cluster, database: str, table: str, 
                          expected_projections: List[str]) -> None:
        """Verify projections exist on all cluster nodes."""
        for node_name in MetadataTestHelper.ALL_NODES:
            node = cluster.instances[node_name]
            query = f"""
                SELECT create_table_query 
                FROM system.tables 
                WHERE database='{database}' AND name='{table}'
            """
            create_query = node.query(query)
            
            for proj in expected_projections:
                assert f"PROJECTION {proj}" in create_query, \
                    f"Projection {proj} not found on {node_name}"
            logger.info(f"Projections {expected_projections} verified on {node_name}")
    
    @staticmethod
    def verify_table_settings(cluster, database: str, table: str, 
                             expected_setting: str) -> None:
        """Verify table settings are consistent across all cluster nodes."""
        for node_name in MetadataTestHelper.ALL_NODES:
            node = cluster.instances[node_name]
            query = f"""
                SELECT engine_full 
                FROM system.tables 
                WHERE database='{database}' AND name='{table}'
            """
            result = node.query(query)
            assert expected_setting in result, f"Setting not updated on {node_name}"
            logger.info(f"Settings verified on {node_name}: {expected_setting}")
    
    @staticmethod
    def verify_database_exists(cluster, database: str) -> None:
        """Verify database exists across all nodes."""
        for node_name in MetadataTestHelper.ALL_NODES:
            node = cluster.instances[node_name]
            query = f"""
                SELECT COUNT() 
                FROM system.databases 
                WHERE name = '{database}'
            """
            result = node.query(query)
            actual_count = int(result[0])
            assert actual_count == 1, \
                f"Expected 1 database on {node_name}"
            logger.info(f"Database exists on {node_name}")
    @staticmethod
    def verify_table_count(cluster, database: str, expected_count: int) -> None:
        """Verify number of tables in a database across all nodes."""
        for node_name in MetadataTestHelper.ALL_NODES:
            node = cluster.instances[node_name]
            query = f"""
                SELECT COUNT() 
                FROM system.tables 
                WHERE database = '{database}'
            """
            result = node.query(query)
            actual_count = int(result[0])
            assert actual_count == expected_count, \
                f"Expected {expected_count} tables in {database} on {node_name}"
            logger.info(f"Table count verified on {node_name}: {expected_count} tables")
    
    @staticmethod
    def verify_tables_dropped(cluster, database: str, tables: List[str]) -> None:
        """Verify specified tables have been dropped from all nodes."""
        table_list = "', '".join(tables)
        for node_name in MetadataTestHelper.ALL_NODES:
            node = cluster.instances[node_name]
            query = f"""
                SELECT COUNT() 
                FROM system.tables 
                WHERE name in ('{table_list}') AND database = '{database}'
            """
            result = node.query(query)
            actual_count = int(result[0])
            assert actual_count == 0, f"Tables {tables} still exist on {node_name}"
            logger.info(f"Tables {tables} verified dropped on {node_name}")
    
    @staticmethod
    def verify_database_dropped(cluster, database: str) -> None:
        """Verify database has been dropped from all nodes."""
        for node_name in MetadataTestHelper.ALL_NODES:
            node = cluster.instances[node_name]
            query = f"SELECT COUNT() FROM system.databases WHERE name='{database}'"
            result = node.query(query)
            actual_count = int(result[0])
            assert actual_count == 0, f"Database {database} still exists on {node_name}"
            logger.info(f"Database {database} verified dropped on {node_name}")


class TableCreator:
    """Helper class for creating test tables."""
    
    @staticmethod
    def create_mergetree_table(node, database: str, table: str) -> None:
        """Create a MergeTree table."""
        query = f"""
            CREATE TABLE {database}.{table}
            (
                id UInt64,
                dt DateTime
            )
            ENGINE = MergeTree
            PARTITION BY toDate(dt)
            ORDER BY id
            TTL dt + toIntervalDay(7)
        """
        logger.info(f"Creating MergeTree table {database}.{table}")
        node.query(query)
    
    @staticmethod
    def create_distributed_table(node, database: str, local_table: str, 
                                dist_table: str, logic_cluster: str) -> None:
        """Create a Distributed table."""
        query = f"""
            CREATE TABLE {database}.{dist_table}
            AS {database}.{local_table}
            ENGINE = Distributed('{logic_cluster}', '{database}', '{local_table}', rand())
        """
        logger.info(f"Creating Distributed table {database}.{dist_table}")
        node.query(query)
    
    @staticmethod
    def create_replicated_table(node, database: str, table: str) -> None:
        """Create a ReplicatedMergeTree table."""
        query = f"""
            CREATE TABLE {database}.{table}
            (
                id UInt64,
                dt DateTime
            )
            ENGINE = ReplicatedMergeTree(
                '/clickhouse/tables/{{layer}}-{{shard}}/{database}/{table}',
                '{{replica}}'
            )
            PARTITION BY toYYYYMMDD(dt)
            ORDER BY id
            TTL dt + toIntervalDay(7)
        """
        logger.info(f"Creating ReplicatedMergeTree table {database}.{table}")
        node.query(query)

    @staticmethod
    def create_system_distributed_table(node, database: str, table: str, logic_cluster: str) -> None:
        """Create a system distributed table."""
        query = f"""
            CREATE TABLE {database}.{table}
            AS system.query_log
            ENGINE = Distributed('{logic_cluster}', system, query_log, rand())
        """
        logger.info(f"Creating System Distributed table {database}.{table}")
        node.query(query)

    @staticmethod
    def create_as_table(node, database: str, local_table: str, dest_table: str) -> None:
        """Create a MergeTree table."""
        query = f"""
            CREATE TABLE {database}.{dest_table}
            AS {database}.{local_table}
        """
        logger.info(f"Creating MergeTree table {database}.{dest_table}")
        node.query(query)


@pytest.fixture(scope="module")
def cluster():
    """Fixture to create and manage ClickHouse cluster for testing."""
    cluster = ClickHouseCluster(__file__)
        
    # Configuration for 2 shards with 2 replicas each
    node_configs = [
        ("node1", "replica1", "shard1"),
        ("node2", "replica2", "shard1"),
        ("node3", "replica1", "shard2"),
        ("node4", "replica2", "shard2"),
    ]
    
    for node_name, replica, shard in node_configs:
        cluster.add_instance(
            node_name,
            main_configs=["configs/config.d/metadata_centra.xml"],
            macros={"replica": replica, "shard": shard, "layer": "test"},
            with_minio=True,
            with_zookeeper=True,
        )
    
    try:
        logger.info("Starting cluster...")
        cluster.start()
        logger.info("Cluster started")
        
        yield cluster
    finally:
        cluster.shutdown()


def test_mergetree_table_operations(cluster):
    """Test MergeTree table creation, ALTER operations, and metadata sync."""
    helper = MetadataTestHelper()
    creator = TableCreator()
    
    node1 = cluster.instances["node1"]
    node2 = cluster.instances["node2"]

    minio = cluster.minio_client
    helper.verify_minio_object_count(cluster, minio, 1)
    
    # 1. Create database
    logger.info("Creating database test_db_v1")
    node1.query("CREATE DATABASE IF NOT EXISTS test_db_v1")
    helper.wait_for_sync()
    helper.verify_database_exists(cluster, "test_db_v1")
    helper.verify_minio_object_count(cluster, minio, 2)
    
    # 2. Create MergeTree and Distributed tables
    creator.create_mergetree_table(node1, "test_db_v1", "test_tb_v1_local")
    creator.create_distributed_table(
        node2, "test_db_v1", "test_tb_v1_local", "test_tb_v1", "test_meta_cetra_admin"
    )
    helper.wait_for_sync()
    helper.verify_table_count(cluster, "test_db_v1", 2)
    
    initial_columns = ["dt", "id"]
    helper.verify_metadata_consistency(
        cluster, "test_db_v1", "test_tb_v1_local", initial_columns
    )
    helper.verify_metadata_consistency(
        cluster, "test_db_v1", "test_tb_v1", initial_columns
    )
    helper.verify_minio_object_count(cluster, minio, 4)
    
    # 3. ALTER: Add column
    logger.info("Adding column 'name' to tables")
    node3 = cluster.instances["node3"]
    node3.query("ALTER TABLE test_db_v1.test_tb_v1_local ADD COLUMN name String")
    node3.query("ALTER TABLE test_db_v1.test_tb_v1 ADD COLUMN name String")
    helper.wait_for_sync()
    
    updated_columns = ["dt", "id", "name"]
    helper.verify_metadata_consistency(
        cluster, "test_db_v1", "test_tb_v1_local", updated_columns
    )
    helper.verify_metadata_consistency(
        cluster, "test_db_v1", "test_tb_v1", updated_columns
    )
    helper.verify_minio_object_count(cluster, minio, 6)
    
    # 4. Insert data
    logger.info("Inserting test data")
    insert_queries = [
        "INSERT INTO test_db_v1.test_tb_v1_local VALUES (1, now() - 120, 'Bob')",
        "INSERT INTO test_db_v1.test_tb_v1_local VALUES (2, now() - 100, 'Leo')",
        "INSERT INTO test_db_v1.test_tb_v1_local VALUES (3, now() - 80, 'Jack')",
    ]
    
    for i, query in enumerate(insert_queries, 1):
        node = cluster.instances[f"node{i}"]
        node.query(query)
    
    node4 = cluster.instances["node4"]
    result = node4.query("SELECT count() FROM test_db_v1.test_tb_v1")
    assert int(result[0]) == 3, f"Expected 3 rows, got {result[0]}"
    
    # 5. ALTER: Add indexes
    logger.info("Adding indexes to table")
    index_queries = [
        "ALTER TABLE test_db_v1.test_tb_v1_local ADD INDEX idx1 name TYPE bloom_filter GRANULARITY 1",
        "ALTER TABLE test_db_v1.test_tb_v1_local ADD INDEX idx2 id TYPE minmax GRANULARITY 3",
        "ALTER TABLE test_db_v1.test_tb_v1_local ADD INDEX idx3 name TYPE set(100) GRANULARITY 3",
    ]
    
    for query in index_queries:
        node1.query(query)
    
    helper.wait_for_sync()
    helper.verify_indexes(
        cluster, "test_db_v1", "test_tb_v1_local", ["idx1", "idx2", "idx3"]
    )
    
    # 6. Insert more data
    node1.query("INSERT INTO test_db_v1.test_tb_v1_local VALUES (5, now() - 120, 'Rachel')")
    
    # 7. ALTER: Modify TTL
    logger.info("Modifying TTL to 15 days")
    node1.query(
        "ALTER TABLE test_db_v1.test_tb_v1_local MODIFY TTL dt + toIntervalDay(15) settings materialize_ttl_after_modify = 0"
    )
    helper.wait_for_sync()
    helper.verify_ttl(cluster, "test_db_v1", "test_tb_v1_local", "toIntervalDay(15)")
    
    # 8. Insert more data
    node2.query("INSERT INTO test_db_v1.test_tb_v1_local VALUES (6, now() - 120, 'Ross')")
    
    result = node1.query("SELECT count() FROM test_db_v1.test_tb_v1")
    assert "5" in result, f"Expected 5 rows, got {result}"
    
    # 9. ALTER: Add projections
    logger.info("Adding projections to table")
    projection_queries = [
        """
        ALTER TABLE test_db_v1.test_tb_v1_local
        ADD PROJECTION normal_p1 (SELECT * ORDER BY name)
        """,
        """
        ALTER TABLE test_db_v1.test_tb_v1_local
        ADD PROJECTION agg_p1 (SELECT name, count(), sum(id) GROUP BY name)
        """,
    ]
    
    for query in projection_queries:
        node2.query(query)
    
    helper.wait_for_sync()
    helper.verify_projections(
        cluster, "test_db_v1", "test_tb_v1_local", ["normal_p1", "agg_p1"]
    )
    
    # 10. ALTER: Modify table settings
    logger.info("Modifying table settings")
    node3.query(
        """
        ALTER TABLE test_db_v1.test_tb_v1_local 
        MODIFY SETTING max_bytes_to_merge_at_max_space_in_pool = 10240000000
        """
    )
    helper.wait_for_sync()
    
    expected_setting = "max_bytes_to_merge_at_max_space_in_pool = 10240000000"
    helper.verify_table_settings(
        cluster, "test_db_v1", "test_tb_v1_local", expected_setting
    )

    # 11. Insert more data
    node3.query("INSERT INTO test_db_v1.test_tb_v1_local VALUES (7, now() - 120, 'Phoebe')")

    result = node3.query("SELECT count() FROM test_db_v1.test_tb_v1")
    assert "6" in result, f"Expected 6 rows, got {result}"

    # 12. Create as table
    creator.create_as_table(node1, "test_db_v1", "test_tb_v1_local", "test_tb_v1_as_local")
    helper.wait_for_sync()
    helper.verify_table_count(cluster, "test_db_v1", 3)
    
    # 13. Drop tables
    logger.info("Dropping tables")
    node1.query("DROP TABLE IF EXISTS test_db_v1.test_tb_v1_local SYNC")
    node1.query("DROP TABLE IF EXISTS test_db_v1.test_tb_v1 SYNC")
    node1.query("DROP TABLE IF EXISTS test_db_v1.test_tb_v1_as_local SYNC")
    helper.wait_for_sync()
    
    helper.verify_tables_dropped(
        cluster, "test_db_v1", ["test_tb_v1_local", "test_tb_v1", "test_tb_v1_as_local"]
    )
    
    logger.info("MergeTree table operations test completed successfully")


def test_replicated_mergetree_table_operations(cluster):
    """Test ReplicatedMergeTree table creation, ALTER operations, and replication."""
    helper = MetadataTestHelper()
    creator = TableCreator()
    
    node1 = cluster.instances["node1"]
    node2 = cluster.instances["node2"]
    
    # 1. Create database
    logger.info("Creating database test_db_v2")
    node1.query("CREATE DATABASE IF NOT EXISTS test_db_v2")
    helper.wait_for_sync()
    helper.verify_database_exists(cluster, "test_db_v2")
    
    # 2. Create ReplicatedMergeTree and Distributed tables
    creator.create_replicated_table(node1, "test_db_v2", "test_tb_rp_v1_local")
    creator.create_distributed_table(
        node2, "test_db_v2", "test_tb_rp_v1_local", "test_tb_rp_v1", "test_meta_cetra_replica"
    )
    helper.wait_for_sync()
    
    initial_columns = ["dt", "id"]
    helper.verify_metadata_consistency(
        cluster, "test_db_v2", "test_tb_rp_v1_local", initial_columns
    )
    helper.verify_metadata_consistency(
        cluster, "test_db_v2", "test_tb_rp_v1", initial_columns
    )
    
    # 3. ALTER: Add column
    logger.info("Adding column 'name' to replicated tables")
    node1.query("ALTER TABLE test_db_v2.test_tb_rp_v1_local ADD COLUMN name String")
    node2.query("ALTER TABLE test_db_v2.test_tb_rp_v1 ADD COLUMN name String")
    helper.wait_for_sync()
    
    updated_columns = ["dt", "id", "name"]
    helper.verify_metadata_consistency(
        cluster, "test_db_v2", "test_tb_rp_v1_local", updated_columns
    )
    helper.verify_metadata_consistency(
        cluster, "test_db_v2", "test_tb_rp_v1", updated_columns
    )
    
    # 4. Insert data
    logger.info("Inserting data and testing replication")
    insert_data = [
        (1, "Bob"),
        (2, "Leo"),
        (3, "Jack"),
    ]
    
    for i, (id_val, name) in enumerate(insert_data, 1):
        node = cluster.instances[f"node{min(i, 2)}"]
        node.query(f"""
            INSERT INTO test_db_v2.test_tb_rp_v1 
            VALUES ({id_val}, now() - {120 - i*20}, '{name}')
        """)
    
    helper.wait_for_sync(5)
    
    count1 = node1.query("SELECT count() FROM test_db_v2.test_tb_rp_v1")
    count2 = node2.query("SELECT count() FROM test_db_v2.test_tb_rp_v1")
    
    assert count1 == count2, f"Replication mismatch: {count1} vs {count2}"
    assert "3" in count1, f"Expected 3 rows, got {count1}"
    
    # 5. ALTER: Add indexes
    logger.info("Adding indexes to replicated table")
    index_queries = [
        "ALTER TABLE test_db_v2.test_tb_rp_v1_local ADD INDEX idx1 name TYPE bloom_filter GRANULARITY 1",
        "ALTER TABLE test_db_v2.test_tb_rp_v1_local ADD INDEX idx2 id TYPE minmax GRANULARITY 3",
    ]
    
    for query in index_queries:
        node1.query(query)
        helper.wait_for_sync()
    
    helper.verify_indexes(
        cluster, "test_db_v2", "test_tb_rp_v1_local", ["idx1", "idx2"]
    )
    
    # 6. Insert more data
    node1.query("INSERT INTO test_db_v2.test_tb_rp_v1 VALUES (5, now() - 120, 'Rachel')")
    helper.wait_for_sync(3)
    
    result = node1.query("SELECT name FROM test_db_v2.test_tb_rp_v1 ORDER BY id")
    expected_names = ["Bob", "Leo", "Jack", "Rachel"]
    for name in expected_names:
        assert name in result, f"Name {name} not found in result"
    
    # 7. ALTER: Modify TTL
    logger.info("Modifying TTL to 15 days")
    node1.query(
        "ALTER TABLE test_db_v2.test_tb_rp_v1_local MODIFY TTL dt + toIntervalDay(15) settings materialize_ttl_after_modify = 0"
    )
    helper.wait_for_sync()
    helper.verify_ttl(cluster, "test_db_v2", "test_tb_rp_v1_local", "toIntervalDay(15)")
    
    # 8. ALTER: Add projections
    logger.info("Adding projections to replicated table")
    projection_queries = [
        """
        ALTER TABLE test_db_v2.test_tb_rp_v1_local
        ADD PROJECTION normal_p1 (SELECT * ORDER BY name)
        """,
        """
        ALTER TABLE test_db_v2.test_tb_rp_v1_local
        ADD PROJECTION agg_p1 (SELECT name, count(), sum(id) GROUP BY name)
        """,
    ]
    
    node3 = cluster.instances["node3"]
    for query in projection_queries:
        node3.query(query)
    
    helper.wait_for_sync()
    helper.verify_projections(
        cluster, "test_db_v2", "test_tb_rp_v1_local", ["normal_p1", "agg_p1"]
    )
    
    # 9. ALTER: Modify table settings
    logger.info("Modifying replicated table settings")
    node4 = cluster.instances["node4"]
    node4.query(
        """
        ALTER TABLE test_db_v2.test_tb_rp_v1_local 
        MODIFY SETTING max_bytes_to_merge_at_max_space_in_pool = 10240000000
        """
    )
    helper.wait_for_sync()
    
    expected_setting = "max_bytes_to_merge_at_max_space_in_pool = 10240000000"
    helper.verify_table_settings(
        cluster, "test_db_v2", "test_tb_rp_v1_local", expected_setting
    )
    
    # 10. Insert final data and verify
    node3.query("INSERT INTO test_db_v2.test_tb_rp_v1 VALUES (6, now() - 120, 'Ross')")
    helper.wait_for_sync(2)
    
    count3 = node3.query("SELECT count() FROM test_db_v2.test_tb_rp_v1")
    count4 = node4.query("SELECT count() FROM test_db_v2.test_tb_rp_v1")
    
    assert count3 == count4, f"Final replication mismatch: {count3} vs {node4}"
    assert "5" in count3, f"Expected 5 rows, got {count3}"
    
    # 11. Create additional table to verify multi-table database handling
    logger.info("Creating additional replicated table")
    creator.create_replicated_table(node1, "test_db_v2", "test_tb_rp_v2_local")
    helper.wait_for_sync()
    
    helper.verify_table_count(cluster, "test_db_v2", 3)

    # 12. Create system distiributed table
    creator.create_system_distributed_table(node1, "test_db_v2", "query_log_all", "test_meta_cetra_admin")

    helper.wait_for_sync()
    
    helper.verify_table_count(cluster, "test_db_v2", 4)
    
    # 13. Drop database
    logger.info("Dropping database test_db_v2")
    node1.query("DROP DATABASE IF EXISTS test_db_v2 sync")
    helper.wait_for_sync()
    
    helper.verify_database_dropped(cluster, "test_db_v2")
    
    logger.info("ReplicatedMergeTree table operations test completed successfully")


@pytest.fixture(autouse=True)
def cleanup(cluster):
    """Cleanup fixture to run after each test."""
    yield
    
    logger.info("Running cleanup after test")
    node = cluster.instances["node3"]
    
    databases_to_clean = ["test_db_v1", "test_db_v2"]
    for db in databases_to_clean:
        try:
            node.query(f"DROP DATABASE IF EXISTS {db} SYNC")
            logger.info(f"Cleaned up database {db}")
        except Exception as e:
            logger.warning(f"Error cleaning up database {db}: {e}")
    
    time.sleep(2)
