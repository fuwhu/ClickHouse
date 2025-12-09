import subprocess
import tempfile
from pathlib import Path

import pytest
import requests

from helpers.cluster import ClickHouseCluster
from test_data_parts_receive.protocol import build_payload

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance("node")


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def get_part_info(node, database, table):
    result = node.query(
        f"""
        SELECT name, path
        FROM system.parts
        WHERE database = '{database}'
          AND table = '{table}'
          AND active
        LIMIT 1
        FORMAT TabSeparated
        """
    )
    if not result.strip():
        return None, None
    
    parts = result.strip().split('\t')
    return parts[0], parts[1]


def send_part(node, database, table, part_name, part_path):
    # Copy part from container to temporary directory on host
    with tempfile.TemporaryDirectory() as temp_dir:
        local_part_path = Path(temp_dir) / part_name
        
        # Copy the part directory from container to host
        subprocess.run(
            ["docker", "cp", f"{node.docker_id}:{part_path}", str(local_part_path)],
            check=True,
            capture_output=True
        )
        
        payload = build_payload(part_name, local_part_path)
        
        url = f"http://{node.ip_address}:9009/"
        params = {
            'endpoint': f'DataPartsReceive:{database}.{table}',
            'part': part_name,
            'client_protocol_version': '0',
            'compress': 'false'
        }
        
        response = requests.post(
            url,
            params=params,
            data=payload,
            headers={'Content-Type': 'application/octet-stream'},
            timeout=30
        )
        
        return response


def test_basic_receive(started_cluster):
    # Create source table and insert data
    node.query("DROP TABLE IF EXISTS test_source")
    node.query("""
        CREATE TABLE test_source (
            id UInt32,
            name String,
            value UInt64
        ) ENGINE = MergeTree()
        ORDER BY id
        SETTINGS min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0
    """)
    
    node.query("""
        INSERT INTO test_source VALUES
            (1, 'apple', 100),
            (2, 'banana', 200),
            (3, 'cherry', 300),
            (4, 'date', 400),
            (5, 'elderberry', 500)
    """)
    
    node.query("OPTIMIZE TABLE test_source FINAL")
    
    part_name, part_path = get_part_info(node, "default", "test_source")
    assert part_name is not None, "No active part found in test_source"
    
    # Create target table with enable_data_parts_receive_service
    node.query("DROP TABLE IF EXISTS test_target")
    node.query("""
        CREATE TABLE test_target (
            id UInt32,
            name String,
            value UInt64
        ) ENGINE = MergeTree()
        ORDER BY id
        SETTINGS enable_data_parts_receive_service = 1
    """)
    
    # Send part to DataPartsReceive service
    response = send_part(node, "default", "test_target", part_name, part_path)
    assert response.status_code == 200, f"Failed to send part: {response.status_code} - {response.text}"
    
    node.query(f"ALTER TABLE test_target ATTACH PART '{part_name}'")
    
    source_data = node.query("SELECT * FROM test_source ORDER BY id")
    target_data = node.query("SELECT * FROM test_target ORDER BY id")
    assert source_data == target_data, "Data mismatch after attach"
    
    assert node.query("SELECT count() FROM test_target") == "5\n"

    node.query("DROP TABLE test_source")
    node.query("DROP TABLE test_target")