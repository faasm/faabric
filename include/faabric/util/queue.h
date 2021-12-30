#pragma once

#include <faabric/util/exception.h>
#include <faabric/util/locks.h>
#include <faabric/util/logging.h>

#include <boost/circular_buffer.hpp>
#include <condition_variable>
#include <queue>
#include <readerwriterqueue/readerwritercircularbuffer.h>

#define DEFAULT_QUEUE_TIMEOUT_MS 5000
#define DEFAULT_QUEUE_SIZE 1024

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

    T dequeue(long timeoutMs = DEFAULT_QUEUE_TIMEOUT_MS)
    {
        UniqueLock lock(mx);

        if (timeoutMs <= 0) {
            SPDLOG_ERROR("Invalid queue timeout: {} <= 0", timeoutMs);
            throw std::runtime_error("Invalid queue timeout");
        }

        while (mq.empty()) {
            std::cv_status returnVal = enqueueNotifier.wait_for(
              lock, std::chrono::milliseconds(timeoutMs));

            // Work out if this has returned due to timeout expiring
            if (returnVal == std::cv_status::timeout) {
                throw QueueTimeoutException("Timeout waiting for dequeue");
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

// Wrapper around moodycamel's blocking fixed capacity single producer single
// consumer queue
template<typename T>
class FixedCapacityQueue
{
  public:
    FixedCapacityQueue(int capacity)
      : mq(capacity){};

    FixedCapacityQueue()
      : mq(DEFAULT_QUEUE_SIZE){};

    void enqueue(T value, long timeoutMs = DEFAULT_QUEUE_TIMEOUT_MS)
    {
        if (timeoutMs <= 0) {
            SPDLOG_ERROR("Invalid queue timeout: {} <= 0", timeoutMs);
            throw std::runtime_error("Invalid queue timeout");
        }

        bool success =
          mq.wait_enqueue_timed(std::move(value), timeoutMs * 1000);
        if (!success) {
            throw QueueTimeoutException("Timeout waiting for enqueue");
        }
    }

    void dequeueIfPresent(T* res) { mq.try_dequeue(*res); }

    T dequeue(long timeoutMs = DEFAULT_QUEUE_TIMEOUT_MS)
    {
        if (timeoutMs <= 0) {
            SPDLOG_ERROR("Invalid queue timeout: {} <= 0", timeoutMs);
            throw std::runtime_error("Invalid queue timeout");
        }

        T value;
        bool success = mq.wait_dequeue_timed(value, timeoutMs * 1000);
        if (!success) {
            throw QueueTimeoutException("Timeout waiting for dequeue");
        }

        return value;
    }

    T* peek(long timeoutMs = DEFAULT_QUEUE_TIMEOUT_MS)
    {
        throw std::runtime_error("Peek not implemented");
    }

    void drain(long timeoutMs = DEFAULT_QUEUE_TIMEOUT_MS)
    {
        T value;
        bool success;
        while (size() > 0) {
            success = mq.wait_dequeue_timed(value, timeoutMs * 1000);
            if (!success) {
                throw QueueTimeoutException("Timeout waiting to drain");
            }
        }
    }

    long size() { return mq.size_approx(); }

    void reset()
    {
        moodycamel::BlockingReaderWriterCircularBuffer<T> empty(
          mq.max_capacity());
        std::swap(mq, empty);
    }

  private:
    moodycamel::BlockingReaderWriterCircularBuffer<T> mq;
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
