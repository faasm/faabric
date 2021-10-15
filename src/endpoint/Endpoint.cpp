#include <faabric/endpoint/Endpoint.h>
#include <faabric/scheduler/Scheduler.h>
#include <faabric/util/logging.h>
#include <faabric/util/macros.h>
#include <faabric/util/timing.h>

#include <optional>
#include <signal.h>
#include <stdexcept>
#include <thread>
#include <typeinfo>
#include <vector>

namespace faabric::endpoint {

namespace detail {
struct EndpointState
{
    EndpointState(int threadCountIn)
      : ioc(threadCountIn)
    {}
    asio::io_context ioc;
    std::vector<std::thread> ioThreads;
};
}

namespace {
class HttpConnection : public std::enable_shared_from_this<HttpConnection>
{
    asio::io_context& ioc;
    beast::tcp_stream stream;
    beast::flat_buffer buffer;
    beast::http::request_parser<beast::http::string_body> parser;
    std::shared_ptr<HttpRequestHandler> handler;

  public:
    HttpConnection(asio::io_context& iocIn,
                   asio::ip::tcp::socket&& socket,
                   std::shared_ptr<HttpRequestHandler> handlerIn)
      : ioc(iocIn)
      , stream(std::move(socket))
      , buffer()
      , parser()
      , handler(handlerIn)
    {}

    void run()
    {
        asio::dispatch(stream.get_executor(),
                       beast::bind_front_handler(&HttpConnection::doRead,
                                                 this->shared_from_this()));
    }

  private:
    void doRead()
    {
        parser.body_limit(boost::none);
        faabric::util::SystemConfig& conf = faabric::util::getSystemConfig();
        stream.expires_after(std::chrono::seconds(conf.globalMessageTimeout));
        beast::http::async_read(
          stream,
          buffer,
          parser,
          beast::bind_front_handler(&HttpConnection::onRead,
                                    this->shared_from_this()));
    }

    void handleRequest(faabric::util::BeastHttpRequest msg)
    {
        HttpRequestContext hrc{ ioc,
                                stream.get_executor(),
                                beast::bind_front_handler(
                                  &HttpConnection::sendResponse,
                                  this->shared_from_this()) };
        handler->onRequest(std::move(hrc), std::move(msg));
    }

    void onRead(beast::error_code ec, size_t bytesTransferred)
    {
        UNUSED(bytesTransferred);
        if (ec == beast::http::error::end_of_stream) {
            doClose();
            return;
        }
        if (ec) {
            SPDLOG_ERROR("Error reading an HTTP request: {}", ec.message());
            return;
        }
        SPDLOG_TRACE("Read HTTP request of {} bytes", bytesTransferred);
        handleRequest(parser.release());
    }

    void sendResponse(faabric::util::BeastHttpResponse&& response)
    {
        // response needs to be freed after the send completes
        auto ownedResponse = std::make_unique<faabric::util::BeastHttpResponse>(
          std::move(response));
        ownedResponse->prepare_payload();
        beast::http::async_write(
          stream,
          *ownedResponse,
          beast::bind_front_handler(&HttpConnection::onWrite,
                                    this->shared_from_this(),
                                    std::move(ownedResponse)));
    }

    void onWrite(std::unique_ptr<faabric::util::BeastHttpResponse> response,
                 beast::error_code ec,
                 size_t bytesTransferred)
    {
        bool needsEof = response->need_eof();
        response.reset();
        UNUSED(bytesTransferred);
        if (ec) {
            SPDLOG_ERROR("Couldn't write HTTP response: {}", ec.message());
            return;
        }
        SPDLOG_TRACE("Write HTTP response of {} bytes", bytesTransferred);
        if (needsEof) {
            doClose();
            return;
        }
        // reset parser to a fresh object, it has no copy/move assignment
        parser.~parser();
        new (&parser) decltype(parser)();
        doRead();
    }

    void doClose()
    {
        beast::error_code ec;
        stream.socket().shutdown(asio::socket_base::shutdown_send, ec);
        // ignore errors on connection closing
    }
};

class EndpointListener : public std::enable_shared_from_this<EndpointListener>
{
    asio::io_context& ioc;
    asio::ip::tcp::acceptor acceptor;
    std::shared_ptr<HttpRequestHandler> handler;

  public:
    EndpointListener(asio::io_context& iocIn,
                     asio::ip::tcp::endpoint endpoint,
                     std::shared_ptr<HttpRequestHandler> handlerIn)
      : ioc(iocIn)
      , acceptor(asio::make_strand(iocIn))
      , handler(handlerIn)
    {
        try {
            acceptor.open(endpoint.protocol());
            acceptor.set_option(asio::socket_base::reuse_address(true));
            acceptor.bind(endpoint);
            acceptor.listen(asio::socket_base::max_listen_connections);
        } catch (std::runtime_error& e) {
            SPDLOG_CRITICAL(
              "Couldn't listen on port {}: {}", endpoint.port(), e.what());
            throw;
        }
    }

    void run()
    {
        asio::dispatch(acceptor.get_executor(),
                       beast::bind_front_handler(&EndpointListener::doAccept,
                                                 this->shared_from_this()));
    }

  private:
    void doAccept()
    {
        // create a new strand (forces all related tasks to happen on one
        // thread)
        acceptor.async_accept(
          asio::make_strand(ioc),
          beast::bind_front_handler(&EndpointListener::handleAccept,
                                    this->shared_from_this()));
    }

    void handleAccept(beast::error_code ec, asio::ip::tcp::socket socket)
    {
        if (ec) {
            SPDLOG_ERROR("Failed accept(): {}", ec.message());
        } else {
            std::make_shared<HttpConnection>(ioc, std::move(socket), handler)
              ->run();
        }
        doAccept();
    }
};
}

Endpoint::Endpoint(int portIn,
                   int threadCountIn,
                   std::shared_ptr<HttpRequestHandler> requestHandlerIn)
  : port(portIn)
  , threadCount(threadCountIn)
  , state(nullptr)
  , requestHandler(requestHandlerIn)
{}

Endpoint::~Endpoint() {}

struct SchedulerMonitoringTask
  : public std::enable_shared_from_this<SchedulerMonitoringTask>
{
    asio::io_context& ioc;
    asio::deadline_timer timer;

    SchedulerMonitoringTask(asio::io_context& ioc)
      : ioc(ioc)
      , timer(ioc, boost::posix_time::milliseconds(1))
    {}

    void run()
    {
        faabric::scheduler::getScheduler().updateMonitoring();
        timer.expires_at(timer.expires_at() +
                         boost::posix_time::milliseconds(500));
        timer.async_wait(
          std::bind(&SchedulerMonitoringTask::run, this->shared_from_this()));
    }
};

void Endpoint::start(bool awaitSignal)
{
    SPDLOG_INFO("Starting HTTP endpoint on {}, {} threads", port, threadCount);

    this->state = std::make_unique<detail::EndpointState>(this->threadCount);

    const auto address = asio::ip::make_address_v4("0.0.0.0");
    const auto port = static_cast<uint16_t>(this->port);

    std::make_shared<EndpointListener>(state->ioc,
                                       asio::ip::tcp::endpoint{ address, port },
                                       this->requestHandler)
      ->run();

    std::optional<asio::signal_set> signals;
    if (awaitSignal) {
        signals.emplace(state->ioc, SIGINT, SIGTERM);
        signals->async_wait([&](beast::error_code const& ec, int sig) {
            if (!ec) {
                SPDLOG_INFO("Received signal: {}", sig);
                state->ioc.stop();
            }
        });
    }

    std::make_shared<SchedulerMonitoringTask>(state->ioc)->run();

    int extraThreads = std::max(awaitSignal ? 0 : 1, this->threadCount - 1);
    state->ioThreads.reserve(extraThreads);
    auto ioc_run = [&ioc{ state->ioc }]() {
        try {
            ioc.run();
        } catch (std::exception& ex) {
            SPDLOG_CRITICAL("Asio runner caught exception of type {}: {}",
                            typeid(ex).name(),
                            ex.what());
            throw;
        }
    };
    for (int i = 0; i < extraThreads; i++) {
        state->ioThreads.emplace_back(ioc_run);
    }
    if (awaitSignal) {
        ioc_run();
    }
}

void Endpoint::stop()
{
    SPDLOG_INFO("Shutting down endpoint on {}", port);
    state->ioc.stop();
    for (auto& thread : state->ioThreads) {
        thread.join();
    }
    state->ioThreads.clear();
}
}
