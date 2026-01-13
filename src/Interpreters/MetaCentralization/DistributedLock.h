#pragma once

#include <chrono>

#include <Common/logger_useful.h>
#include <Common/ZooKeeper/IKeeper.h>
#include <Common/ZooKeeper/Types.h>
#include <Common/ZooKeeper/ZooKeeper.h>

namespace DB
{

/// Distributed lock implementation using ZooKeeper ephemeral nodes
class DistributedLock
{
public:
    DistributedLock(
        zkutil::ZooKeeperPtr zookeeper_,
        const String & lock_path_,
        const String & lock_name_,
        std::chrono::milliseconds timeout_ms_ = std::chrono::milliseconds(30000));

    ~DistributedLock();

    DistributedLock(const DistributedLock &) = delete;
    DistributedLock & operator=(const DistributedLock &) = delete;

    DistributedLock(DistributedLock &&) noexcept = default;
    DistributedLock & operator=(DistributedLock &&) noexcept = default;

    bool tryLock();

    void unlock();

    bool isLocked() const { return is_locked; }

private:
    zkutil::ZooKeeperPtr zookeeper;
    String lock_path;
    String lock_name;
    String lock_node_path;
    std::chrono::milliseconds timeout_ms;
    bool is_locked = false;
    LoggerPtr log;
};

using DistributedLockPtr = std::unique_ptr<DistributedLock>;

}

