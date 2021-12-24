#pragma once

#include <faabric/util/exception.h>
#include <faabric/util/locks.h>
#include <faabric/util/logging.h>

#include <boost/lockfree/spsc_queue.hpp>
#include <condition_variable>
#include <queue>

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

// Single-consumer, single-producer fixed-size queue
template<typename T>
class SpscQueue
{
  public:
    SpscQueue(int capacity)
      : mq(capacity){};

    SpscQueue()
      : mq(DEFAULT_QUEUE_SIZE){};

    void enqueue(T value, long timeoutMs = DEFAULT_QUEUE_TIMEOUT_MS)
    {
        if (!mq.push(value)) {
            UniqueLock lock(mx);

            writerWaiting.store(true, std::memory_order_relaxed);

            while (!mq.push(value)) {
                std::cv_status returnVal = notFullNotifier.wait_for(
                  lock, std::chrono::milliseconds(timeoutMs));

                if (returnVal == std::cv_status::timeout) {
                    throw QueueTimeoutException("Timeout waiting for enqueue");
                }
            }

            writerWaiting.store(false, std::memory_order_relaxed);
        }

        if (readerWaiting.load(std::memory_order_relaxed)) {
            UniqueLock lock(mx);
            notEmptyNotifier.notify_one();
        }
    }

    T dequeue(long timeoutMs = DEFAULT_QUEUE_TIMEOUT_MS)
    {
        T value;

        if (!mq.pop(value)) {
            UniqueLock lock(mx);

            readerWaiting.store(true, std::memory_order_relaxed);

            while (!mq.pop(value)) {
                std::cv_status returnVal = notEmptyNotifier.wait_for(
                  lock, std::chrono::milliseconds(timeoutMs));

                if (returnVal == std::cv_status::timeout) {
                    throw QueueTimeoutException("Timeout waiting for dequeue");
                }
            }

            readerWaiting.store(false, std::memory_order_relaxed);
        }

        if (writerWaiting.load(std::memory_order_relaxed)) {
            UniqueLock lock(mx);
            notFullNotifier.notify_one();
        }

        return value;
    }

    T* peek(long timeoutMs = DEFAULT_QUEUE_TIMEOUT_MS)
    {
        if (mq.read_available() == 0) {
            UniqueLock lock(mx);

            readerWaiting.store(true, std::memory_order_relaxed);

            while (mq.read_available() == 0) {
                std::cv_status returnVal = notEmptyNotifier.wait_for(
                  lock, std::chrono::milliseconds(timeoutMs));

                if (returnVal == std::cv_status::timeout) {
                    throw QueueTimeoutException("Timeout waiting for dequeue");
                }
            }

            readerWaiting.store(false, std::memory_order_relaxed);
        }

        return &mq.front();
    }

    long size() { return mq.read_available(); }

  private:
    boost::lockfree::spsc_queue<T> mq;
    std::mutex mx;
    std::atomic<bool> writerWaiting = false;
    std::condition_variable notEmptyNotifier;
    std::atomic<bool> readerWaiting = false;
    std::condition_variable notFullNotifier;
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
