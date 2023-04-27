#pragma once

#include <faabric/proto/faabric.pb.h>
#include <faabric/util/asio.h>

#include <future>
#include <sys/eventfd.h>

// TODO: move to include/faabric/message/MessageResultPromise.h and move there
// the remainders of faabric.pb.h

namespace faabric::util {
/**
 * A promise for a future message result with an associated eventfd for use with
 * asio.
 */
class MessageResultPromise final
{
  public:
    MessageResultPromise();

    MessageResultPromise(const MessageResultPromise&) = delete;

    inline MessageResultPromise(MessageResultPromise&& other)
    {
        this->operator=(std::move(other));
    }

    MessageResultPromise& operator=(const MessageResultPromise&) = delete;

    inline MessageResultPromise& operator=(MessageResultPromise&& other)
    {
        this->promise = std::move(other.promise);
        this->eventFd = other.eventFd;
        other.eventFd = -1;
        return *this;
    }

    ~MessageResultPromise();

    void setValue(std::shared_ptr<faabric::Message>& msg);

    std::promise<std::shared_ptr<faabric::Message>> promise;
    int eventFd = -1;
};

class MessageResultPromiseAwaiter
  : public std::enable_shared_from_this<MessageResultPromiseAwaiter>
{
  public:
    MessageResultPromiseAwaiter(std::shared_ptr<faabric::Message> msg,
                                std::shared_ptr<MessageResultPromise> mrp,
                                asio::posix::stream_descriptor dsc,
                                std::function<void(faabric::Message&)> handler)
      : msg(msg)
      , mrp(std::move(mrp))
      , dsc(std::move(dsc))
      , handler(handler)
    {}

    ~MessageResultPromiseAwaiter()
    {
        // Ensure that Asio doesn't close the eventfd, to prevent a
        // double-close in the MLR destructor
        dsc.release();
    }

    // Schedule this task waiting on the eventfd in the Asio queue
    void doAwait();

    void await(const boost::system::error_code& ec);

  private:
    std::shared_ptr<faabric::Message> msg;
    std::shared_ptr<MessageResultPromise> mrp;
    asio::posix::stream_descriptor dsc;
    std::function<void(faabric::Message&)> handler;
};
}
