#include <catch2/catch.hpp>

#include "DummyExecutor.h"
#include "DummyExecutorFactory.h"
#include "faabric_utils.h"
#include "fixtures.h"

#include <faabric/endpoint/FaabricEndpoint.h>
#include <faabric/scheduler/Scheduler.h>
#include <faabric/util/json.h>

#include <pthread.h>

static int portIn = 9090;

namespace tests {

using namespace faabric::endpoint;

/*
 * Very simple endpoint handler to test the endpoint functionality
 */
class TestEndpointHandler final
  : public faabric::endpoint::HttpRequestHandler
  , public std::enable_shared_from_this<TestEndpointHandler>
{
  public:
    void onRequest(faabric::endpoint::HttpRequestContext&& ctx,
                   faabric::util::BeastHttpRequest&& request) override
    {
        std::string requestStr = request.body();

        faabric::util::BeastHttpResponse response;

        if (requestStr.empty()) {
            response.result(beast::http::status::bad_request);
            response.body() = std::string("Empty request");
        } else if (requestStr == "ping") {
            response.result(beast::http::status::ok);
            response.body() = std::string("pong");
        } else {
            response.result(beast::http::status::bad_request);
            response.body() = std::string("Bad request body");
        }

        return ctx.sendFunction(std::move(response));
    }
};

class EndpointTestFixture
{
  public:
    EndpointTestFixture()
      : host(LOCALHOST)
      , port(++portIn)
      , endpoint(port, 4, std::make_shared<TestEndpointHandler>())
    {
        endpoint.start(faabric::endpoint::EndpointMode::BG_THREAD);
    }

    ~EndpointTestFixture() { endpoint.stop(); }

  protected:
    std::string host;
    int port;
    FaabricEndpoint endpoint;

    // Test case state
    boost::beast::http::status expectedReturnCode;
    std::string expectedResponseBody;

    std::pair<int, std::string> doPost(const std::string& body)
    {
        return postToUrl(host, port, body);
    }
};

void* doWork(void* arg)
{
    FaabricEndpoint endpoint(9080, 4, std::make_shared<TestEndpointHandler>());
    endpoint.start(EndpointMode::SIGNAL);

    SPDLOG_INFO("Exitting..");

    endpoint.stop();

    pthread_exit(0);
}

TEST_CASE("Test starting an endpoint in signal mode", "[endpoint]")
{
    // Use pthreads to be able to signal the thread correctly
    pthread_t ptid;

    pthread_create(&ptid, nullptr, &doWork, nullptr);

    // Send a post request to make sure endpoint is running
    std::pair<int, std::string> result = postToUrl(LOCALHOST, 9080, "ping");
    REQUIRE(boost::beast::http::int_to_status(result.first) ==
            boost::beast::http::status::ok);
    REQUIRE(result.second == "pong");

    pthread_kill(ptid, SIGINT);

    pthread_join(ptid, nullptr);
}

TEST_CASE_METHOD(EndpointTestFixture,
                 "Test posting a request to the endpoint",
                 "[endpoint]")
{
    std::string requestBody;

    SECTION("Valid request")
    {
        requestBody = "ping";
        expectedReturnCode = boost::beast::http::status::ok;
        expectedResponseBody = "pong";
    }

    SECTION("Empty request")
    {
        requestBody = "";
        expectedReturnCode = boost::beast::http::status::bad_request;
        expectedResponseBody = "Empty request";
    }

    SECTION("Invalid request")
    {
        requestBody = "pong";
        expectedReturnCode = boost::beast::http::status::bad_request;
        expectedResponseBody = "Bad request body";
    }

    std::pair<int, std::string> result = doPost(requestBody);
    REQUIRE(boost::beast::http::int_to_status(result.first) ==
            expectedReturnCode);
    REQUIRE(result.second == expectedResponseBody);
}
}
