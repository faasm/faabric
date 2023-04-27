#include <faabric/util/MessageResultPromise.h>

namespace faabric::util {

MessageResultPromise::MessageResultPromise()
{
    eventFd = eventfd(0, EFD_CLOEXEC);
}

MessageResultPromise::~MessageResultPromise()
{
    if (eventFd >= 0) {
        close(eventFd);
    }
}

void MessageResultPromise::setValue(std::shared_ptr<faabric::Message>& msg)
{
    this->promise.set_value(msg);
    eventfd_write(this->eventFd, (eventfd_t)1);
}

void MessageResultPromiseAwaiter::await(const boost::system::error_code& ec)
{
    if (!ec) {
        auto msg = mrp->promise.get_future().get();
        handler(*msg);
    } else {
        // The waiting task can spuriously wake up, requeue if this
        // happens
        doAwait();
    }
}

void MessageResultPromiseAwaiter::doAwait()
{
    dsc.async_wait(asio::posix::stream_descriptor::wait_read,
                   beast::bind_front_handler(
                     &MessageResultPromiseAwaiter::await,
                     this->shared_from_this()));
}
}
