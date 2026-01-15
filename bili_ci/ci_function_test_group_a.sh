#!/bin/bash
set -euo pipefail

echo "Starting ZooKeeper..."
/opt/zookeeper/bin/zkServer.sh start

echo "Waiting for ZooKeeper to be ready..."
for i in {1..10}; do
    if echo ruok | nc -w 1 localhost 2181 2>/dev/null | grep -q imok; then
        echo "ZooKeeper is up."
        break
    fi
    echo "ZooKeeper not ready yet, retrying ($i/10)..."
    sleep 2
    if [[ $i -eq 10 ]]; then
        echo "ZooKeeper failed to start."
        exit 1
    fi
done

echo "Copying ClickHouse binary to /usr/bin..."
cp build/programs/clickhouse /usr/bin

echo "Creating symlinks..."
ln -s /usr/bin/clickhouse /usr/bin/clickhouse-server
ln -s /usr/bin/clickhouse /usr/bin/clickhouse-client
ln -s /usr/bin/clickhouse /usr/bin/clickhouse-format
ln -s /usr/bin/clickhouse /usr/bin/clickhouse-local

echo "Starting ClickHouse server..."
nohup /usr/bin/clickhouse-server --config /etc/clickhouse-server/config.xml > clickhouse-server.log 2>&1 &

echo "Waiting for ClickHouse ports (9000, 8123) to be ready..."
for i in {1..30}; do
    tcp_ok=0
    http_ok=0

    if nc -z localhost 9000 2>/dev/null; then
        tcp_ok=1
    fi
    if nc -z localhost 8123 2>/dev/null; then
        http_ok=1
    fi

    if [[ $tcp_ok -eq 1 && $http_ok -eq 1 ]]; then
        echo "ClickHouse is up (9000 & 8123 are open)."
        break
    fi

    echo "ClickHouse not ready yet, retrying ($i/30)..."
    sleep 2

    if [[ $i -eq 30 ]]; then
        echo "ClickHouse failed to start (ports not open)."
        echo "Check clickhouse-server.log for details."
        tail -n 50 clickhouse-server.log || true
        exit 1
    fi
done

echo "Running clickhouse-test..."

./tests/clickhouse-test -j 40 create alter group 01600_log_queries_with_extensive_info 03682_log_queries_where_group_by_columns \
    --skip s3 \
    02228_merge_tree_insert_memory_usage \
    02285_executable_user_defined_function_group_by \
    02488_zero_copy_detached_parts_drop_table \
    02943_alter_user_modify_profiles_and_settings \
    01169_old_alter_partition_isolation_stress \
    03237_insert_sparse_columns_mem \
    01169_alter_partition_isolation_stress \
    01174_select_insert_isolation \
    02835_drop_user_during_session \
    03008_azure_plain_rewritable_alter_partition \
    02870_move_partition_to_volume_io_throttling \
    03373_named_session_try_recreate_before_timeout \
    01171_mv_select_insert_isolation_long \
    03161_clickhouse_keeper_client_create_sequential \
    02943_alter_user_modify_settings \
    03095_group_by_server_constants_bug \
    01170_alter_partition_isolation \
    03582_pr_read_in_order_hits \
    02808_filesystem_cache_drop_query \
    02286_drop_filesystem_cache \
    01281_group_by_limit_memory_tracking \
    00173_group_by_use_nulls \
    00152_insert_different_granularity \
    00151_order_by_read_in_order \
    00081_group_by_without_key_and_totals \
    00078_group_by_arrays \
    00052_group_by_in \
    00021_1_select_with_in \
    00021_2_select_with_in \
    00021_3_select_with_in

echo "ClickHouse test finished."
