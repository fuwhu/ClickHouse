import time

import pytest

import helpers.client as client
from helpers.client import QueryRuntimeException
from helpers.cluster import ClickHouseCluster
from helpers.test_tools import TSV

@pytest.fixture(scope="module")
def cluster():
    cluster = ClickHouseCluster(__file__)
    
    node_configs = [
        ("node1", "shard1", "default:", "test_cluster_three_shards", "test_cluster_two_shards"),
        ("node2", "shard1", "default:", "test_cluster_three_shards", "test_cluster_two_shards"),
        ("node3", "shard2", "zookeeper3:", "test_cluster_three_shards", "test_cluster_two_shards"),
        ("node4", "shard2", "zookeeper3:", "test_cluster_three_shards", "test_cluster_two_shards"),
        ("node5", "shard3", "zookeeper2:", "test_cluster_three_shards", "test_cluster_two_shards"),
        ("node6", "shard3", "zookeeper2:", "test_cluster_three_shards", "test_cluster_two_shards"),
    ]

    for node_name, shard, zookeeper, three_shards, two_shards in node_configs:
        cluster.add_instance(
            node_name,
            main_configs=["configs/zookeeper_config.xml", "configs/remote_servers.xml"],
            macros={"replica": node_name, "shard": shard, "zookeeper": zookeeper, "three_shards": three_shards, "two_shards": two_shards , "layer": "test"},
            with_zookeeper=True,
        )

    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def drop_table(nodes, table_name):
    for node in nodes:
        node.query("DROP TABLE IF EXISTS {} NO DELAY".format(table_name))


def test_create_replicated_table_with_zookeeper(cluster):
    node1 = cluster.instances["node1"]
    node2 = cluster.instances["node2"]
    node3 = cluster.instances["node3"]
    node4 = cluster.instances["node4"]
    node5 = cluster.instances["node5"]
    node6 = cluster.instances["node6"]

    drop_table([node1, node2, node3, node4, node5, node6], "test_macro_zookeeper")
    for node in [node1, node2, node3, node4, node5, node6]:
        node.query(
            """
                CREATE TABLE test_macro_zookeeper (a Int32)
                ENGINE = ReplicatedMergeTree('{zookeeper}/clickhouse/tables/test_macro_zookeeper/{layer}-{shard}', '{replica}')
                ORDER BY a;
            """
        )

    zk1 = cluster.get_kazoo_client("zoo1")
    assert zk1.exists("/clickhouse/tables/test_macro_zookeeper/test-shard1")
    assert zk1.exists("/clickhouse/tables/test_macro_zookeeper/test-shard1/replicas/node1")
    assert zk1.exists("/clickhouse/tables/test_macro_zookeeper/test-shard1/replicas/node2")

    assert zk1.exists("/clickhouse/tables/test_macro_zookeeper/test-shard2")
    assert zk1.exists("/clickhouse/tables/test_macro_zookeeper/test-shard2/replicas/node3")
    assert zk1.exists("/clickhouse/tables/test_macro_zookeeper/test-shard2/replicas/node4")

    assert zk1.exists("/clickhouse/tables/test_macro_zookeeper/test-shard3")
    assert zk1.exists("/clickhouse/tables/test_macro_zookeeper/test-shard3/replicas/node5")
    assert zk1.exists("/clickhouse/tables/test_macro_zookeeper/test-shard3/replicas/node6")


    drop_table([node1, node2, node3, node4, node5, node6], "test_macro_zookeeper_three_shards")
    for node in [node1, node2, node3, node4, node5, node6]:
        node.query(
            """
                CREATE TABLE test_macro_zookeeper_three_shards (a Int32)
                ENGINE = Distributed('{three_shards}', default, test_macro_zookeeper, rand())
            """
        )
    

    drop_table([node1, node2, node3, node4], "test_macro_zookeeper_two_shards")
    for node in [node1, node2, node3, node4]:
        node.query(
            """
                CREATE TABLE test_macro_zookeeper_two_shards (a Int32)
                ENGINE = Distributed('{two_shards}', default, test_macro_zookeeper, rand())
            """
        )

    node1.query("INSERT INTO test_macro_zookeeper select rand() from numbers(1000)")
    node3.query("INSERT INTO test_macro_zookeeper select rand() from numbers(2000)")
    node5.query("INSERT INTO test_macro_zookeeper select rand() from numbers(3000)")
    
    time.sleep(5)
    
    count1 = node2.query("SELECT count() FROM test_macro_zookeeper_three_shards")
    count2 = node4.query("SELECT count() FROM test_macro_zookeeper_three_shards")
    
    assert count1 == count2, f"Final replication mismatch: {node2} vs {node4}"
    assert "6000" in count1, f"Expected 6000 rows, got {count1}"

    count3 = node1.query("SELECT count() FROM test_macro_zookeeper_two_shards")
    count4 = node3.query("SELECT count() FROM test_macro_zookeeper_two_shards")
    
    assert count3 == count4, f"Final replication mismatch: {node1} vs {node3}"
    assert "3000" in count3, f"Expected 3000 rows, got {count3}"

    drop_table([node1, node2, node3, node4, node5, node6], "test_macro_zookeeper")

    assert not zk1.exists("/clickhouse/tables/test_macro_zookeeper/test-shard1")
    assert not zk1.exists("/clickhouse/tables/test_macro_zookeeper/test-shard2")
    assert not zk1.exists("/clickhouse/tables/test_macro_zookeeper/test-shard3")

    drop_table([node1, node2, node3, node4, node5, node6], "test_macro_zookeeper_three_shards")
    drop_table([node1, node2, node3, node4], "test_macro_zookeeper_two_shards")
