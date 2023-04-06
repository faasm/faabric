#include <catch2/catch.hpp>

#include "faabric_utils.h"
#include "fixtures.h"

#include <faabric/planner/planner.pb.h>
#include <faabric/util/json.h>

#include <boost/beast/http/status.hpp>
#include <google/protobuf/util/json_util.h>

using namespace faabric::planner;

/* This tests use that the planner is already running as a standalone service
 * and only test the external HTTP API
 */
namespace tests {
class FaabricPlannerEndpointTestFixture
  : public ConfTestFixture
{
  public:
    FaabricPlannerEndpointTestFixture()
      : host(conf.plannerHost)
      , port(conf.plannerPort)
    {}

  protected:
    // Planner endpoint state
    std::string host;
    int port;

    // Test case state
    boost::beast::http::status expectedReturnCode;
    std::string expectedResponseBody;
    std::string msgJsonStr;

    std::pair<int, std::string> doPost(const std::string& body)
    {
        return postToUrl(host, port, body);
    }
};

TEST_CASE_METHOD(FaabricPlannerEndpointTestFixture,
                 "Test malformed requests",
                 "[planner]")
{
    // TODO
    ;
}

TEST_CASE_METHOD(FaabricPlannerEndpointTestFixture,
                 "Test planner reset",
                 "[planner]")
{
    expectedReturnCode = boost::beast::http::status::ok;
    expectedResponseBody = "Planner fully reset!";

    // TODO: check that the host count is actually 0

    HttpMessage msg;
    msg.set_type(HttpMessage_Type_RESET);

    faabric::util::messageToJsonPb(msg, &msgJsonStr);

    std::pair<int, std::string> result = doPost(msgJsonStr);
    REQUIRE(boost::beast::http::int_to_status(result.first) == expectedReturnCode);
    REQUIRE(result.second == expectedResponseBody);
}

TEST_CASE_METHOD(FaabricPlannerEndpointTestFixture,
                 "Test flushing available hosts",
                 "[planner]")
{
    expectedReturnCode = boost::beast::http::status::ok;
    expectedResponseBody = "Flushed available hosts!";

    // TODO: check that the host count is actually 0

    HttpMessage msg;
    msg.set_type(HttpMessage_Type_FLUSH_HOSTS);

    // TODO: move this to src/util/json.cpp
    google::protobuf::util::Status status = google::protobuf::util::MessageToJsonString(msg, &msgJsonStr);
    if (!status.ok()) {
        SPDLOG_ERROR("Serialising JSON to message failed: {}", status.message().data());
        throw std::runtime_error("Error serialising!");
    }

    std::pair<int, std::string> result = doPost(msgJsonStr);
    REQUIRE(boost::beast::http::int_to_status(result.first) == expectedReturnCode);
    REQUIRE(result.second == expectedResponseBody);
}
}
