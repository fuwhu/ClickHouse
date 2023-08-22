import time
import pytest
import logging
from helpers.cluster import ClickHouseCluster


cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance(
    "node1",
    main_configs=["configs/zookeeper_config.xml", "configs/remote_servers.xml"],
    with_zookeeper=True,
    use_keeper=False,
    stay_alive=True,
)
node2 = cluster.add_instance(
    "node2",
    main_configs=["configs/zookeeper_config.xml", "configs/remote_servers.xml"],
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
        print(ex)

    finally:
        cluster.shutdown()


def test_create_insert_select(started_cluster):
    for i, node in enumerate([node1, node2]):
        node.query("DROP TABLE IF EXISTS test_unique_engine SYNC")
        node.query(
            """
                CREATE TABLE test_unique_engine (id UInt64, name String, version DateTime)
                ENGINE = ReplicatedMergeTree('/clickhouse/tables/0/test_unique_engine', '{replica}', version)
                ORDER BY id
                UNIQUE KEY name
                SETTINGS unique_key_index_type = 1;
            """.format(
                replica=node.name
            )
        )

    node1.query("INSERT INTO test_unique_engine VALUES ('1', 'Jack', '2023-08-21 10:00:00'), ('2', 'Leo', '2023-08-21 10:00:00')")
    node2.query("INSERT INTO test_unique_engine VALUES ('2', 'Leo', '2023-08-21 10:05:00'), ('2', 'Leo', '2023-08-21 10:10:00');")
    node1.query("INSERT INTO test_unique_engine VALUES ('3', 'Rachel', '2023-08-21 10:00:00'), ('1', 'Jack', '2023-08-21 09:00:00')")
    
    time.sleep(5)

    assert node1.query("select count() from test_unique_engine").strip() == "3"
    assert node2.query("select count() from test_unique_engine").strip() == "3"

    assert node1.query("select version from test_unique_engine where name = 'Jack'").strip() == "2023-08-21 10:00:00"
    assert node2.query("select version from test_unique_engine where name = 'Jack'").strip() == "2023-08-21 10:00:00"
    assert node1.query("select version from test_unique_engine where name = 'Leo'").strip() == "2023-08-21 10:10:00"
    assert node2.query("select version from test_unique_engine where name = 'Leo'").strip() == "2023-08-21 10:10:00"
    assert node1.query("select version from test_unique_engine where name = 'Rachel'").strip() == "2023-08-21 10:00:00"
    assert node2.query("select version from test_unique_engine where name = 'Rachel'").strip() == "2023-08-21 10:00:00"
