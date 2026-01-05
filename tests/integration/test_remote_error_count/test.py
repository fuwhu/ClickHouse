import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import TSV, assert_eq_with_retry

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance(
    "node",
    main_configs=["configs/remote_servers.xml"],
    stay_alive=True,
)

node_1 = cluster.add_instance(
    "node_1",
    main_configs=["configs/remote_servers.xml"],
    stay_alive=True,
    with_zookeeper=True,
)

node_2 = cluster.add_instance(
    "node_2",
    main_configs=["configs/remote_servers.xml"],
    stay_alive=True,
    with_zookeeper=True,
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        node_1.query("CREATE TABLE test_remote (id UInt32) ENGINE=MergeTree ORDER BY id")
        node_1.query("INSERT INTO test_remote SELECT number FROM numbers(10)")

        node_2.query("CREATE TABLE test_remote (id UInt32) ENGINE=MergeTree ORDER BY id")
        node_2.query("INSERT INTO test_remote SELECT number FROM numbers(10)")

        node.query("CREATE TABLE dist_test_remote (id UInt32) ENGINE=Distributed('test_cluster', 'default', 'test_remote')")
        node.query("CREATE TABLE dist_src (id UInt32) ENGINE=Distributed('test_cluster', 'default', 'test_remote')")

        node_1.query(
            "CREATE TABLE IF NOT EXISTS lazy_local (id UInt32) "
            "ENGINE=ReplicatedMergeTree('/clickhouse/tables/remote_error_lazy', 'replica1') ORDER BY id"
        )
        node_2.query(
            "CREATE TABLE IF NOT EXISTS lazy_local (id UInt32) "
            "ENGINE=ReplicatedMergeTree('/clickhouse/tables/remote_error_lazy', 'replica2') ORDER BY id"
        )
        node_1.query(
            "CREATE TABLE IF NOT EXISTS dist_lazy "
            "AS lazy_local ENGINE=Distributed('test_cluster', 'default', 'lazy_local')"
        )

        yield cluster
    finally:
        cluster.shutdown()


def get_remote_error_counts(instance=node):
    result = instance.query(
        """
        SELECT host_name, port, remote_error_count
        FROM system.clusters
        WHERE cluster='test_cluster'
        ORDER BY host_name, port
        """
    )
    return {(row[0], int(row[1])): int(row[2]) for row in TSV.toMat(result)}


def run_remote_exception_query(use_hedged_requests, async_socket_for_remote):
    node.query_and_get_error(
        """
        SELECT count() FROM dist_test_remote WHERE throwIf(id >= 0, 'boom')
        SETTINGS
            load_balancing = 'in_order',
            load_balancing_first_offset = 0,
            max_parallel_replicas = 1,
            allow_experimental_parallel_reading_from_replicas = 0,
            async_socket_for_remote = {},
            async_query_sending_for_remote = 1,
            use_hedged_requests = {},
            hedged_connection_timeout_ms = 100000,
            skip_unavailable_shards = 0
        """.format(int(async_socket_for_remote), int(use_hedged_requests))
    )


def run_distributed_write_exception(async_socket_for_remote):
    node.query_and_get_error(
        """
        INSERT INTO dist_test_remote
        SELECT id FROM dist_src
        WHERE throwIf(1, 'boom')
        SETTINGS
            parallel_distributed_insert_select = 2,
            async_socket_for_remote = {},
            async_query_sending_for_remote = 1
        """.format(int(async_socket_for_remote))
    )


def run_lazy_remote_exception_query(instance):
    instance.query_and_get_error(
        """
        SELECT count() FROM dist_lazy WHERE throwIf(id >= 0, 'boom')
        SETTINGS
            prefer_localhost_replica = 1,
            max_replica_delay_for_distributed_queries = 1,
            fallback_to_stale_replicas_for_distributed_queries = 1,
            allow_experimental_parallel_reading_from_replicas = 0,
            async_socket_for_remote = 1,
            async_query_sending_for_remote = 1,
            use_hedged_requests = 0
        """
    )


def test_remote_error_count_hedged_connection_sync(started_cluster):
    node.restart_clickhouse()
    
    before = get_remote_error_counts()

    run_remote_exception_query(use_hedged_requests=True, async_socket_for_remote=False)

    after = get_remote_error_counts()

    assert sum(after.values()) == sum(before.values()) + 1
    assert any(after[host] > before[host] for host in after)


def test_remote_error_count_hedged_connection_async(started_cluster):
    node.restart_clickhouse()
    
    before = get_remote_error_counts()

    run_remote_exception_query(use_hedged_requests=True, async_socket_for_remote=True)

    after = get_remote_error_counts()

    assert sum(after.values()) == sum(before.values()) + 1
    assert any(after[host] > before[host] for host in after)


def test_remote_error_count_multiplexed_connection_sync(started_cluster):
    node.restart_clickhouse()

    before = get_remote_error_counts()

    run_remote_exception_query(use_hedged_requests=False, async_socket_for_remote=False)

    after = get_remote_error_counts()

    assert sum(after.values()) == sum(before.values()) + 1
    assert any(after[host] > before[host] for host in after)


def test_remote_error_count_multiplexed_connection_async(started_cluster):
    node.restart_clickhouse()

    before = get_remote_error_counts()

    run_remote_exception_query(use_hedged_requests=False, async_socket_for_remote=True)

    after = get_remote_error_counts()

    assert sum(after.values()) == sum(before.values()) + 1
    assert any(after[host] > before[host] for host in after)


def test_remote_error_count_distributed_insert_select_sync(started_cluster):
    node.restart_clickhouse()

    before = get_remote_error_counts()

    run_distributed_write_exception(async_socket_for_remote=False)

    after = get_remote_error_counts()

    assert sum(after.values()) == sum(before.values()) + 1
    assert any(after[host] > before[host] for host in after)


def test_remote_error_count_distributed_insert_select_async(started_cluster):
    node.restart_clickhouse()

    before = get_remote_error_counts()

    run_distributed_write_exception(async_socket_for_remote=True)

    after = get_remote_error_counts()

    assert sum(after.values()) == sum(before.values()) + 1
    assert any(after[host] > before[host] for host in after)


def test_remote_error_count_lazy_remote_read(started_cluster):
    node_1.restart_clickhouse()
    node_2.restart_clickhouse()

    node_1.query("TRUNCATE TABLE lazy_local")
    node_2.query("TRUNCATE TABLE lazy_local")

    node_1.query("SYSTEM STOP FETCHES")
    node_2.query("INSERT INTO lazy_local SELECT number FROM numbers(10)")

    try:
        assert_eq_with_retry(
            node_1,
            "SELECT (absolute_delay > 0) FROM system.replicas WHERE table = 'lazy_local'",
            "1",
        )

        before = get_remote_error_counts(instance=node_1)

        run_lazy_remote_exception_query(node_1)

        after = get_remote_error_counts(instance=node_1)

        assert sum(after.values()) == sum(before.values()) + 1
        assert any(after[host] > before[host] for host in after)
    finally:
        node_1.query("SYSTEM START FETCHES")
        node_1.query("SYSTEM SYNC REPLICA lazy_local")
