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

echo "Creating ck_log and ck_data directories..."
mkdir -p /data/ck_log/ci /data/ck_data/ci

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

./tests/clickhouse-test create alter select drop group ttl \
    --skip s3 \
    02521_lightweight_delete_and_ttl \
    02100_alter_scalar_circular_deadlock \
    03373_named_session_try_recreate_before_timeout \
    02488_zero_copy_detached_parts_drop_table \
    01414_freeze_does_not_prevent_alters \
    03008_azure_plain_rewritable_alter_partition \
    02437_drop_mv_restart_replicas \
    03394_pr_insert_select \
    03095_group_by_server_constants_bug \
    01170_alter_partition_isolation \
    02454_create_table_with_custom_disk \
    02417_keeper_map_create_drop \
    01413_alter_update_supertype \
    00652_mutations_alter_update \
    01171_mv_select_insert_isolation_long \
    02439_merge_selecting_partitions \
    03143_group_by_constant_secondary \
    01169_old_alter_partition_isolation_stress \
    01169_alter_partition_isolation_stress \
    02835_drop_user_during_session \
    02943_alter_user_modify_settings \
    03161_clickhouse_keeper_client_create_sequential \
    02441_alter_delete_and_drop_column \
    02792_drop_projection_lwd \
    02285_executable_user_defined_function_group_by \
    02117_show_create_table_system \
    02870_move_partition_to_volume_io_throttling \
    02445_replicated_db_alter_partition \
    03442_alter_delete_empty_part \
    03394_pr_insert_select_local_pipeline \
    03008_deduplication_remote_insert_select \
    02832_alter_delete_indexes_projections \
    03442_alter_delete_empty_part_rmt \
    02149_schema_inference_create_table_syntax \
    02943_alter_user_modify_profiles_and_settings \
    01174_select_insert_isolation \
    03394_pr_insert_select_threads \
    03229_async_insert_alter \
    03229_async_insert_alter_http \
    02808_filesystem_cache_drop_query \
    02447_drop_database_replica \
    02286_drop_filesystem_cache \
    02262_column_ttl \
    02227_test_create_empty_sqlite_db \
    02222_create_table_without_columns_metadata \
    01281_group_by_limit_memory_tracking \
    00173_group_by_use_nulls \
    00081_group_by_without_key_and_totals \
    00078_group_by_arrays \
    00052_group_by_in \
    00021_2_select_with_in \
    00021_1_select_with_in \
    00021_3_select_with_in

echo "ClickHouse test finished."