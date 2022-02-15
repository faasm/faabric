#include <catch2/catch.hpp>

#include "faabric_utils.h"

#include <faabric/util/json.h>

#include <faabric/util/logging.h>

using namespace faabric::util;

namespace tests {
TEST_CASE("Test message to JSON round trip", "[util]")
{
    faabric::Message msg;
    msg.set_type(faabric::Message_MessageType_FLUSH);
    msg.set_user("user 1");
    msg.set_function("great function");
    msg.set_executedhost("blah.host.blah");
    msg.set_finishtimestamp(123456543);

    msg.set_pythonuser("py user");
    msg.set_pythonfunction("py func");
    msg.set_pythonentry("py entry");

    msg.set_isasync(true);
    msg.set_ispython(true);
    msg.set_isstatusrequest(true);
    msg.set_isexecgraphrequest(true);

    msg.set_ismpi(true);
    msg.set_mpiworldid(1234);
    msg.set_mpirank(5678);
    msg.set_mpiworldsize(33);

    msg.set_cmdline("some cmdline");

    msg.set_recordexecgraph(true);
    auto& map = *msg.mutable_execgraphdetails();
    map["foo"] = "bar";
    auto& intMap = *msg.mutable_intexecgraphdetails();
    intMap["foo"] = 0;

    msg.set_migrationcheckperiod(33);

    msg.set_topologyhint("TEST_TOPOLOGY_HINT");

    SECTION("Dodgy characters") { msg.set_inputdata("[0], %$ 2233 9"); }

    SECTION("Bytes")
    {
        std::vector<uint8_t> bytes = { 0, 0, 1, 1, 0, 2, 2, 3, 3, 4, 4 };
        msg.set_inputdata(bytes.data(), bytes.size());
    }

    faabric::util::setMessageId(msg);

    REQUIRE(msg.id() > 0);
    REQUIRE(msg.timestamp() > 0);

    std::string jsonString = faabric::util::messageToJson(msg);

    faabric::Message actual = faabric::util::jsonToMessage(jsonString);

    checkMessageEquality(msg, actual);
}

TEST_CASE("Test get JSON property from JSON string", "[util]")
{
    // Valid lookups
    REQUIRE(getValueFromJsonString("foo", "{\"foo\": \"bar\"}") == "bar");
    REQUIRE(getValueFromJsonString(
              "foo", "{\"foo\": \"bar\", \"other\": \"value\"}") == "bar");
    REQUIRE(getValueFromJsonString(
              "foo", "{\"other\": \"value\", \"foo\": \"abc\"}") == "abc");

    // Nothing to return
    REQUIRE(getValueFromJsonString(
              "foo", "{\"FOO\": \"aa\", \"blah\": \"xxx\"}") == "");
    REQUIRE(getValueFromJsonString("foo", "{\"foo\": \"\"}") == "");
}

TEST_CASE("Test with raw string literals", "[util]")
{
    const faabric::Message& msg =
      jsonToMessage(R"({"user": "foo", "function": "bar"})");
    REQUIRE(msg.user() == "foo");
    REQUIRE(msg.function() == "bar");
}
}
