import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance(
    "node",
    main_configs=["configs/clusters.xml"],
    stay_alive=True,
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        node.query("CREATE TABLE local_table (id UInt32) ENGINE=MergeTree ORDER BY id")
        node.query("CREATE TABLE dist_table (id UInt32) ENGINE=Distributed('test_cluster', 'default', 'local_table')")
        node.query("INSERT INTO local_table VALUES (1), (2), (3), (4), (5)")
        yield cluster
    finally:
        cluster.shutdown()


def test_skipped_unavailable_shards_header(started_cluster):
    res = node.http_request(
        "",
        method="GET",
        params={
            "query": (
                "SELECT id % 2 AS bucket, sum(id) AS total "
                "FROM dist_table "
                "GROUP BY bucket "
                "ORDER BY bucket "
                "FORMAT TSV"
            ),
            "skip_unavailable_shards": "1",
            "connect_timeout": "1",
            "connections_with_failover_max_tries": "1"
        },
    )

    has_skipped = res.headers.get("X-ClickHouse-Has-Skiped-Unavailable-Shard")
    assert has_skipped == "1", f"Unexpected X-ClickHouse-Has-Skiped-Unavailable-Shard value: {has_skipped}"

    header = res.headers.get("X-ClickHouse-Skipped-Unavailable-Shards")
    assert header is not None, "Expected X-ClickHouse-Skipped-Unavailable-Shards header"

    skipped = sorted(header.split(","))
    assert skipped == ["test_cluster:shard_2:2", "test_cluster:shard_3:3"], (
        f"Unexpected header value: {header}"
    )
