#include <catch2/catch.hpp>

#include "faabric_utils.h"

#include <faabric/util/json.h>
#include <faabric/util/logging.h>

using namespace faabric::util;

namespace tests {
class JsonTestFixture
{
  public:
    JsonTestFixture()
    {
        msg.set_type(faabric::Message_MessageType_FLUSH);
        msg.set_user("user 1");
        msg.set_function("great function");
        msg.set_executedhost("blah.host.blah");
        msg.set_finishtimestamp(123456543);

        msg.set_pythonuser("py user");
        msg.set_pythonfunction("py func");
        msg.set_pythonentry("py entry");

        msg.set_ispython(true);

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

        msg.set_inputdata("foo bar");

        faabric::util::setMessageId(msg);

        REQUIRE(msg.id() > 0);
        REQUIRE(msg.starttimestamp() > 0);
    }

  protected:
    faabric::Message msg;
};

TEST_CASE_METHOD(JsonTestFixture, "Test message to JSON round trip", "[util]")
{
    SECTION("Dodgy characters") { msg.set_inputdata("[0], %$ 2233 9"); }

    SECTION("Bytes")
    {
        std::vector<uint8_t> bytes = { 0, 0, 1, 1, 0, 2, 2, 3, 3, 4, 4 };
        msg.set_inputdata(bytes.data(), bytes.size());
    }

    std::string jsonString = faabric::util::messageToJson(msg);

    faabric::Message actual;
    faabric::util::jsonToMessage(jsonString, &actual);

    checkMessageEquality(msg, actual);
}

TEST_CASE_METHOD(JsonTestFixture, "Test JSON contains required keys", "[util]")
{
    // We consume the generated JSON files from a variety of places, so this
    // test ensures that the keywords we use elsewhere are generated as part
    // of the serialisation process
    std::vector<std::string> requiredKeys = {
        "input_data",        "python",   "py_user",
        "py_func",           "mpi",      "mpi_world_size",
        "record_exec_graph", "start_ts", "finish_ts",
    };
    std::string jsonString = faabric::util::messageToJson(msg);

    for (const auto& key : requiredKeys) {
        // To make sure we are indeed dealing with keywords, we make sure to
        // quote the keywords and append a colon
        std::string jsonKey = fmt::format("\"{}\":", key);
        REQUIRE(jsonString.find(jsonKey) != std::string::npos);
    }

    // Additionally, make sure that the message type is a number. In
    // particular a 3, as we set the message type to FLUSH in the constructor
    std::string flushStr = "\"type\":3,";
    REQUIRE(jsonString.find(flushStr) != std::string::npos);
}
}
