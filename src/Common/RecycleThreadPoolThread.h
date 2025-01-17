#pragma once

#include "Common/ThreadPool.h"

namespace DB
{

template <typename Thread>
class RecycleThreadPoolThread {
    public:
        explicit RecycleThreadPoolThread(size_t max_fill, std::function<void(size_t)> record_fun_);

        ~RecycleThreadPoolThread();

        bool tryPush(ThreadPoolImpl<Thread> * pool);

    private:
        void cleanFunc();

        ConcurrentBoundedQueue<ThreadPoolImpl<Thread> *> queue;
        //TODO get queue.size() when we gather metric rather than update at the hook point.
        //metric will be not correct if it take a long time(or worse never end) to recycle a pool
        std::function<void(size_t)> record_fun;
        ThreadFromGlobalPool recycle_thread;
        std::atomic<bool> stopped = false;
};

}
