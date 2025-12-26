import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import TSV

cluster = ClickHouseCluster(__file__)

node_1_1 = cluster.add_instance(
    "node_1_1",
    main_configs=["configs/remote_servers.xml"],
    user_configs=["configs/users_slow.xml"],
    macros={"shard": "1", "replica": "1"},
    stay_alive=True
)

node_1_2 = cluster.add_instance(
    "node_1_2",
    main_configs=["configs/remote_servers.xml"],
    user_configs=["configs/users.xml"],
    macros={"shard": "1", "replica": "2"},
    stay_alive=True
)

node_2_1 = cluster.add_instance(
    "node_2_1",
    main_configs=["configs/remote_servers.xml"],
    user_configs=["configs/users.xml"],
    macros={"shard": "2", "replica": "1"},
    stay_alive=True
)

node_2_2 = cluster.add_instance(
    "node_2_2",
    main_configs=["configs/remote_servers.xml"],
    user_configs=["configs/users.xml"],
    macros={"shard": "2", "replica": "2"},
    stay_alive=True
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        for node in (node_1_1, node_1_2, node_2_1, node_2_2):
            node.query(
                "CREATE TABLE test_hedged (id UInt32) ENGINE=MergeTree ORDER BY id"
            )
            node.query("INSERT INTO test_hedged SELECT number FROM numbers(100)")

        node_2_1.query(
            "CREATE TABLE test_hedged_dist AS test_hedged ENGINE=Distributed('test_cluster', 'default', 'test_hedged')"
        )

        yield cluster

    finally:
        cluster.shutdown()


def test_slowdowns_count_and_replica_switch(started_cluster):
    if node_2_1.is_built_with_thread_sanitizer():
        pytest.skip("Hedged requests don't work under Thread Sanitizer")

    result = node_2_1.query(
        """
        SELECT
            getMacro('shard') AS shard,
            getMacro('replica') AS replica,
            count() AS cnt
        FROM test_hedged_dist
        GROUP BY shard, replica
        ORDER BY shard, replica
        SETTINGS
            use_hedged_requests = 1,
            hedged_connection_timeout_ms = 50,
            receive_data_timeout_ms = 200,
            allow_changing_replica_until_first_data_packet = 1,
            load_balancing = 'in_order',
            interactive_delay = 10000000
        """
    )
    assert TSV(result) == TSV("1\t2\t100\n2\t1\t100\n")

    result = node_2_1.query(
        """
        SELECT slowdowns_count FROM system.clusters
        WHERE cluster='test_cluster' AND host_name='node_1_1'
        """
    ).strip()
    assert int(result) == 1

    result = node_2_1.query(
        """
        SELECT slowdowns_count FROM system.clusters
        WHERE cluster='test_cluster' AND host_name='node_1_2'
        """
    ).strip()
    assert int(result) == 0