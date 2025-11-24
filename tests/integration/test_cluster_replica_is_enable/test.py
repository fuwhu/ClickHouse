import time
import pytest
from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance(
    "node1",
    main_configs=[
        "configs/config.d/zookeeper_config.xml",
        "configs/config.d/remote_servers.xml"
    ],
    with_zookeeper=True,
    use_keeper=False,
    stay_alive=True,
)

node2 = cluster.add_instance(
    "node2",
    main_configs=[
        "configs/config.d/zookeeper_config.xml",
        "configs/config.d/remote_servers.xml"
    ],
    with_zookeeper=True,
    use_keeper=False,
    stay_alive=True,
)

node3 = cluster.add_instance(
    "node3",
    main_configs=[
        "configs/config.d/zookeeper_config.xml",
        "configs/config.d/remote_servers.xml"
    ],
    with_zookeeper=True,
    use_keeper=False,
    stay_alive=True,
)

node4 = cluster.add_instance(
    "node4",
    main_configs=[
        "configs/config.d/zookeeper_config.xml",
        "configs/config.d/remote_servers.xml"
    ],
    with_zookeeper=True,
    use_keeper=False,
    stay_alive=True,
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    except Exception as ex:
        print(f"test_cluster_replica_is_enable failed: {ex}")
        raise
    finally:
        cluster.shutdown()


def test_partial_replica_disabled(started_cluster):
    result = node1.query(
        """
        SELECT host_name, port 
        FROM system.clusters 
        WHERE cluster = 'test_cluster_partial_disabled'
        ORDER BY host_name
        """
    )
    
    expected = "node1\t9000\nnode3\t9000\n"
    assert result == expected, f"expected: {expected}, actual: {result}"
    
    count_result = node1.query(
        """
        SELECT count() 
        FROM system.clusters 
        WHERE cluster = 'test_cluster_partial_disabled'
        """
    ).strip()
    
    assert count_result == "2", f"expected 2 replicas, actual: {count_result}"
    
    node2_count = node1.query(
        """
        SELECT count() 
        FROM system.clusters 
        WHERE cluster = 'test_cluster_partial_disabled' AND host_name = 'node2'
        """
    ).strip()
    
    assert node2_count == "0", "node2 should not be in the cluster"


def test_multi_shard_mixed_config(started_cluster):
    result = node1.query(
        """
        SELECT shard_num, host_name, replica_num
        FROM system.clusters 
        WHERE cluster = 'test_cluster_multi_shard'
        ORDER BY shard_num, replica_num
        """
    )
    
    expected = "1\tnode1\t1\n2\tnode3\t1\n2\tnode4\t2\n"
    assert result == expected, f"expected: {expected}, actual: {result}"
    
    shard1_count = node1.query(
        """
        SELECT count() 
        FROM system.clusters 
        WHERE cluster = 'test_cluster_multi_shard' AND shard_num = 1
        """
    ).strip()
    
    shard2_count = node1.query(
        """
        SELECT count() 
        FROM system.clusters 
        WHERE cluster = 'test_cluster_multi_shard' AND shard_num = 2
        """
    ).strip()
    
    assert shard1_count == "1", f"Shard 1 should have 1 replica, actual: {shard1_count}"
    assert shard2_count == "2", f"Shard 2 should have 2 replica, actual: {shard2_count}"

def test_internal_replication_with_disabled_replica(started_cluster):
    result = node1.query(
        """
        SELECT host_name 
        FROM system.clusters 
        WHERE cluster = 'test_cluster_internal_replication'
        ORDER BY host_name
        """
    )
    
    assert result == "node1\n", f"expected only node1, actual: {result}"
    
    count_result = node1.query(
        """
        SELECT count() 
        FROM system.clusters 
        WHERE cluster = 'test_cluster_internal_replication'
        """
    ).strip()
    
    assert count_result == "1", f"expected 1 replica, actual: {count_result}"