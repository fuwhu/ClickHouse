#include "Common/RecycleThreadPoolThread.h"

namespace DB
{

template <typename Thread>
RecycleThreadPoolThread<Thread>::RecycleThreadPoolThread(size_t max_fill, std::function<void(size_t)> record_fun_)
    : queue(max_fill)
    , record_fun(record_fun_)
    , recycle_thread(&RecycleThreadPoolThread<Thread>::cleanFunc, this)
{
}

template <typename Thread>
RecycleThreadPoolThread<Thread>::~RecycleThreadPoolThread()
{
    stopped = true;
    queue.finish();
    if (recycle_thread.joinable()) [[likely]]
        recycle_thread.join();
}

template <typename Thread>
bool RecycleThreadPoolThread<Thread>::tryPush(ThreadPoolImpl<Thread> * pool)
{
    return queue.tryPush(pool);
}

template <typename Thread>
void RecycleThreadPoolThread<Thread>::cleanFunc()
{
    while (!stopped || !queue.empty())
    {
        ThreadPoolImpl<Thread> * pool;
        if (queue.pop(pool))
        {
            pool->wait();
            delete pool;
        }
        record_fun(queue.size());
    }
}

template class RecycleThreadPoolThread<std::thread>;
template class RecycleThreadPoolThread<ThreadFromGlobalPool>;

}
