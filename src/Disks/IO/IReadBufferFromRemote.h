#pragma once

#include <IO/SeekableReadBuffer.h>
#include <IO/BufferWithOwnMemory.h>
#include <Common/callOnce.h>

namespace DB
{

class IReadBufferFromRemote : public BufferWithOwnMemory<SeekableReadBufferWithSize>
{
public:
    explicit IReadBufferFromRemote(size_t size_)
        : BufferWithOwnMemory<SeekableReadBufferWithSize>(size_)
    {
    }

    virtual UInt32 getRemoteSeekCount() const { return 0; }
    virtual UInt64 getRemoteSeekTimeCostMicrosecond() const { return 0; }
    virtual UInt64 getRemoteReadBytes() const { return 0; }
    virtual UInt64 getRemoteReadCount() const { return 0; }
    virtual UInt64 getRemoteReadTimeCostMicrosecond() const { return 0; }
    virtual UInt64 getRemoteReadInitWaitCostMicrosecond() const { return 0; }

    virtual UInt64 getLocalReadBytes() const { return 0; }
    virtual UInt64 getLocalReadCount() const { return 0; }
    virtual UInt64 getLocalReadTimeCostMicrosecond() const { return 0; }
    virtual UInt64 getLocalWriteBytes() const { return 0; }
    virtual UInt64 getLocalWriteCount() const { return 0; }
    virtual UInt64 getLocalWriteTimeCostMicrosecond() const { return 0; }

    virtual UInt64 getTotalReadTimeCostMicrosecond() const { return 0; }

    void initRemoteOnlyOnce()
    {
        callOnce(initialize_called, [&] {
            initRemote();

            is_initialized = true;
        });
    }

    bool isInitialized() const
    {
        return is_initialized;
    }

    virtual bool canLazyInit() const { return false; }

private:
    //send multi request(get meta for example) to remote fs in init func.it may take a long time.
    //should call before execute io operation unless canLazyInit is true.
    virtual void initRemote() = 0;

    std::atomic<bool> is_initialized = false;
    OnceFlag initialize_called;
};

}
