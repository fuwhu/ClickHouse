import time
import pytest
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance(
    "node1",
    main_configs=[
        "configs/config.d/storage_configuration.xml",
        "configs/config.d/background_pool_size.xml",
    ],
    stay_alive=True,
    tmpfs=["/hot_disk:size=40M", "/cold_disk:size=200M"],
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    except Exception as ex:
        print(f"test_ttl_move_strategy failed: {ex}")
        raise
    finally:
        cluster.shutdown()


def test_ttl_move_by_time_strategy(started_cluster):
    """
    Verify TTL-based move strategy, when select_data_parts_for_move_by_ttl=1, the oldest TTL part should be moved first
    """
    node1.query("DROP TABLE IF EXISTS ttl_time_test SYNC")
    
    # Create table with TTL move strategy enabled
    node1.query("""
        CREATE TABLE ttl_time_test (
            data String,
            create_time DateTime
        )
        ENGINE = MergeTree()
        ORDER BY create_time
        TTL create_time + INTERVAL 60 DAY TO VOLUME 'cold',
            create_time + INTERVAL 90 DAY DELETE
        SETTINGS  storage_policy = 'ttl_move_policy',
            select_data_parts_for_move_by_ttl = 1;
    """)
    
    # Insert parts with different TTL times
    # Part 1: Oldest TTL, Small size (10MB)
    node1.query("""
        INSERT INTO ttl_time_test 
        SELECT randomString(1048576), now() - INTERVAL 30 DAY
        FROM numbers(10);
    """)
    
    # Part 2: Medium TTL, Large size (20MB)
    node1.query("""
        INSERT INTO ttl_time_test 
        SELECT randomString(1048576), now() - INTERVAL 20 DAY
        FROM numbers(20);
    """)

    # Part 3: Newest TTL, Small size (5MB)
    # hot disk usage exceeds 40 * 0.8 MB, triggering move of this part to cold_disk
    node1.query("""
        INSERT INTO ttl_time_test 
        SELECT randomString(1048576), now() - INTERVAL 10 DAY
        FROM numbers(5);
    """)
    
    # Wait for moves to complete
    time.sleep(10)
    
    # Verify: parts should be moved to cold_disk
    cold_disk_count = int(node1.query("""
        SELECT count()
        FROM system.parts
        WHERE table = 'ttl_time_test' AND active AND disk_name = 'cold_disk'
    """).strip())
    
    assert cold_disk_count > 0, "Some parts should be moved to cold_disk"
    
    # Verify: the oldest TTL part (90 days ago) must be on cold_disk
    oldest_part_disk = node1.query("""
        SELECT disk_name
        FROM system.parts
        ARRAY JOIN move_ttl_info.min AS ttl_min
        WHERE table = 'ttl_time_test' AND active
        ORDER BY ttl_min
        LIMIT 1
    """).strip()
    
    assert oldest_part_disk == "cold_disk", \
        f"With TTL strategy enabled, oldest part should be on cold_disk, but found on {oldest_part_disk}"
    
    # Verify: parts on cold_disk have older TTL than parts on hot_disk
    # Get the newest TTL on cold_disk
    max_ttl_on_cold = node1.query("""
        SELECT maxOrNull(ttl_min)
        FROM (
            SELECT arrayJoin(move_ttl_info.min) AS ttl_min
            FROM system.parts
            WHERE table = 'ttl_time_test' AND active AND disk_name = 'cold_disk'
        )
    """).strip()
    
    # Get the oldest TTL on hot_disk
    min_ttl_on_hot = node1.query("""
        SELECT minOrNull(ttl_min)
        FROM (
            SELECT arrayJoin(move_ttl_info.min) AS ttl_min
            FROM system.parts
            WHERE table = 'ttl_time_test' AND active AND disk_name = 'hot_disk'
        )
    """).strip()
    
    def _has_value(value: str) -> bool:
        return bool(value) and value != r'\N'

    if _has_value(min_ttl_on_hot):  # If there are still parts on hot_disk
        assert _has_value(max_ttl_on_cold), "TTL metadata on cold_disk should not be NULL"
        # The newest part on cold_disk should be older than the oldest part on hot_disk
        assert max_ttl_on_cold < min_ttl_on_hot, \
            f"Parts on cold_disk should have older TTL than parts on hot_disk. " \
            f"Max TTL on cold: {max_ttl_on_cold}, Min TTL on hot: {min_ttl_on_hot}"
    
    node1.query("DROP TABLE IF EXISTS ttl_time_test SYNC")


