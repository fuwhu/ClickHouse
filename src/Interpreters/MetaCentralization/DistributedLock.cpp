#include <Interpreters/MetaCentralization/DistributedLock.h>

namespace DB
{

DistributedLock::DistributedLock(
    zkutil::ZooKeeperPtr zookeeper_,
    const String & lock_path_,
    const String & lock_name_,
    std::chrono::milliseconds timeout_ms_)
    : zookeeper(std::move(zookeeper_))
    , lock_path(lock_path_)
    , lock_name(lock_name_)
    , timeout_ms(timeout_ms_)
    , log(getLogger("DistributedLock"))
{
}

DistributedLock::~DistributedLock()
{
    if (!is_locked)
        return;

    try
    {
        unlock();
    }
    catch (...)
    {
        tryLogCurrentException(log, "Exception while releasing distributed lock in destructor");
    }
}

bool DistributedLock::tryLock()
{
    if (is_locked)
    {
        LOG_WARNING(log, "Lock is already acquired");
        return true;
    }

    try
    {
        if (zookeeper->expired())
        {
            LOG_WARNING(log, "ZooKeeper session expired");
            return false;
        }

        const auto start_time = std::chrono::steady_clock::now();
        const String full_lock_path = lock_path + "/" + lock_name;
        zookeeper->createAncestors(full_lock_path);
        lock_node_path = zookeeper->create(full_lock_path, "", zkutil::CreateMode::EphemeralSequential);

        LOG_DEBUG(log, "Created lock node: {}", lock_node_path);

        while (true)
        {
            const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::steady_clock::now() - start_time);

            if (elapsed >= timeout_ms)
            {
                LOG_WARNING(log, "Timeout while acquiring lock after {} ms", timeout_ms.count());
                zookeeper->tryRemove(lock_node_path);
                return false;
            }

            zkutil::Strings children = zookeeper->getChildren(lock_path);

            if (children.empty())
            {
                LOG_ERROR(log, "No children found in lock path '{}', unexpected state", lock_path);
                zookeeper->tryRemove(lock_node_path);
                return false;
            }

            std::sort(children.begin(), children.end());
            const String current_node_name = lock_node_path.substr(lock_path.size() + 1);

            if (children.front() == current_node_name)
            {
                is_locked = true;
                LOG_INFO(log, "Successfully acquired distributed lock: {}", lock_node_path);
                return true;
            }

            String previous_node;
            for (size_t i = 0; i < children.size(); ++i)
            {
                if (children[i] == current_node_name && i > 0)
                {
                    previous_node = children[i - 1];
                    break;
                }
            }

            if (previous_node.empty())
            {
                LOG_ERROR(log, "Previous node not found for {}", current_node_name);
                zookeeper->tryRemove(lock_node_path);
                return false;
            }

            const String previous_node_path = lock_path + "/" + previous_node;
            zkutil::EventPtr event = std::make_shared<Poco::Event>();

            if (zookeeper->exists(previous_node_path, nullptr, event))
            {
                const auto remaining_time = timeout_ms - elapsed;
                if (remaining_time.count() <= 0)
                {
                    LOG_WARNING(log, "Timeout while waiting for previous node {}", previous_node_path);
                    zookeeper->tryRemove(lock_node_path);
                    return false;
                }

                LOG_TRACE(log, "Waiting for previous node {} to be removed", previous_node_path);
                event->tryWait(remaining_time.count());
            }
        }
    }
    catch (...)  // NOLINT(bugprone-empty-catch)
    {
        tryLogCurrentException(log, fmt::format("Error while acquiring lock '{}'", lock_name));
        return false;
    }
}

void DistributedLock::unlock()
{
    if (!is_locked)
    {
        LOG_WARNING(log, "Attempting to unlock when lock is not held");
        return;
    }

    try
    {
        if (!lock_node_path.empty())
        {
            zookeeper->tryRemove(lock_node_path);
            LOG_INFO(log, "Released distributed lock: {}", lock_node_path);
        }

        is_locked = false;
        lock_node_path.clear();
    }
    catch (...)
    {
        tryLogCurrentException(log, fmt::format("Exception while releasing lock '{}'", lock_name));
        throw;
    }
}

}
