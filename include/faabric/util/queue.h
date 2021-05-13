#pragma once

#include <faabric/util/exception.h>
#include <faabric/util/locks.h>

#include <queue>

namespace faabric::util {
class QueueTimeoutException : public faabric::util::FaabricException
{
  public:
    explicit QueueTimeoutException(std::string message)
      : FaabricException(std::move(message))
    {}
};

template<typename T>
class Queue
{
  public:
    void enqueue(T value)
    {
        UniqueLock lock(mx);

        mq.emplace(std::move(value));

        enqueueNotifier.notify_one();
    }

    void dequeueIfPresent(T* res)
    {
        UniqueLock lock(mx);

        if (!mq.empty()) {
            T value = std::move(mq.front());
            mq.pop();
            emptyNotifier.notify_one();

            *res = value;
        }
    }

    T dequeue(long timeoutMs = 0)
    {
        UniqueLock lock(mx);

        while (mq.empty()) {
            if (timeoutMs > 0) {
                std::cv_status returnVal = enqueueNotifier.wait_for(
                  lock, std::chrono::milliseconds(timeoutMs));

                // Work out if this has returned due to timeout expiring
                if (returnVal == std::cv_status::timeout) {
                    throw QueueTimeoutException("Timeout waiting for dequeue");
                }
            } else {
                enqueueNotifier.wait(lock);
            }
        }

        T value = std::move(mq.front());
        mq.pop();
        emptyNotifier.notify_one();

        return value;
    }

    T* peek(long timeoutMs = 0)
    {
        UniqueLock lock(mx);
        while (mq.empty()) {
            if (timeoutMs > 0) {
                std::cv_status returnVal = enqueueNotifier.wait_for(
                  lock, std::chrono::milliseconds(timeoutMs));

                if (returnVal == std::cv_status::timeout) {
                    throw QueueTimeoutException("Timeout waiting for dequeue");
                }
            } else {
                enqueueNotifier.wait(lock);
            }
        }

        return &mq.front();
    }

    void waitToDrain(long timeoutMs)
    {
        UniqueLock lock(mx);

        while (!mq.empty()) {
            if (timeoutMs > 0) {
                std::cv_status returnVal = emptyNotifier.wait_for(
                  lock, std::chrono::milliseconds(timeoutMs));

                // Work out if this has returned due to timeout expiring
                if (returnVal == std::cv_status::timeout) {
                    throw QueueTimeoutException("Timeout waiting for empty");
                }
            } else {
                emptyNotifier.wait(lock);
            }
        }
    }

    void drain()
    {
        UniqueLock lock(mx);

        while (!mq.empty()) {
            mq.pop();
        }
    }

    long size()
    {
        UniqueLock lock(mx);
        return mq.size();
    }

    void reset()
    {
        UniqueLock lock(mx);

        std::queue<T> empty;
        std::swap(mq, empty);
    }

  private:
    std::queue<T> mq;
    std::condition_variable enqueueNotifier;
    std::condition_variable emptyNotifier;
    std::mutex mx;
};

class TokenPool
{
  public:
    explicit TokenPool(int nTokens);

    int getToken();

    void releaseToken(int token);

    void reset();

    int size();

    int taken();

    int free();

  private:
    int _size;
    Queue<int> queue;
};
}
