import random
import time
from multiprocessing.dummy import Pool
import datetime

import pytest
from helpers.client import QueryRuntimeException
from helpers.cluster import ClickHouseCluster

node_options = dict(
    with_zookeeper=True,
    main_configs=[
        "configs/remote_servers.xml"
    ]
)

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance("node1", macros={"shard": 0, "replica": 1}, **node_options)
node2 = cluster.add_instance("node2", macros={"shard": 0, "replica": 2}, **node_options)
node3 = cluster.add_instance("node3", macros={"shard": 1, "replica": 1}, **node_options)
node4 = cluster.add_instance("node4", macros={"shard": 1, "replica": 2}, **node_options)
nodes = [node1, node2, node3, node4]


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    except Exception as ex:
        print(ex)
    finally:
        cluster.shutdown()


def create_distributed_table(node, table_name):
    sql = """
            CREATE TABLE %(table_name)s_replicated ON CLUSTER test_cluster
            (
                id UInt32,
                bsi BSI
            )
            ENGINE = ReplicatedMergeTree('/clickhouse/tables/test/{shard}/%(table_name)s_replicated', '{replica}')
            ORDER BY id;
        """ % dict(
        table_name=table_name
    )
    node.query(sql)
    sql = """
            CREATE TABLE %(table_name)s ON CLUSTER test_cluster AS %(table_name)s_replicated
            ENGINE = Distributed(test_cluster, default, %(table_name)s_replicated, rand())
        """ % dict(
        table_name=table_name
    )
    node.query(sql)


def drop_distributed_table(node, table_name):
    node.query(
        "DROP TABLE IF EXISTS {} ON CLUSTER test_cluster SYNC".format(table_name)
    )
    node.query(
        "DROP TABLE IF EXISTS {}_replicated ON CLUSTER test_cluster SYNC".format(
            table_name
        )
    )
    time.sleep(1)


def insert(
    node,
    table_name,
    insert_data
):
    try:
        query = "INSERT INTO {}_replicated SELECT 1, bsi_build(u_id, gmv) AS bsi FROM ("
        
        for k,v in insert_data.items():
            query = query + "SELECT " + str(k) + " AS u_id, " + str(v) + " AS gmv " + "UNION ALL "
        
        query = query[:-10]

        query = query + ")"

        node.query(query.format(table_name))
    except QueryRuntimeException as ex:
        if not ignore_exception:
            raise


def select(
    node,
    table_name,
    expected_result=None,
    iterations=1,
    ignore_exception=False,
    poll=None,
):
    for i in range(iterations):
        start_time = time.time()
        while True:
            try:
                r = node.query(
                    "SELECT bitmapCardinality(bsi_ge(bsi_agg, 5)) AS sum_count FROM ( SELECT bsi_add_agg(bsi) AS bsi_agg FROM {})".format(
                        table_name
                    )
                )
                if expected_result:
                    if (
                        r != expected_result
                        and poll
                        and time.time() - start_time < poll
                    ):
                        continue
                    assert r == expected_result
            except QueryRuntimeException as ex:
                if not ignore_exception:
                    raise
            break


def test_select_bsi_ge(started_cluster):
    table_name = "test_bsi_rp"
    drop_distributed_table(node1, table_name)
    try:
        create_distributed_table(node1, table_name)

        d1 = {1:3,2:6}
        insert(node1, table_name, d1)

        d2 = {3:4,4:10,5:7}
        insert(node3, table_name, d2)

        # check that select still works
        select(node1, table_name, "3\n")
    finally:
        drop_distributed_table(node1, table_name)
