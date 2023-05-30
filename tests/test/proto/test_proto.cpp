#include <catch2/catch.hpp>

#include <faabric/proto/faabric.pb.h>
#include <faabric/util/bytes.h>
#include <faabric_utils.h>

namespace tests {
TEST_CASE("Test protobuf classes", "[proto]")
{
    faabric::Message funcCall;

    std::string user = "foobar user";
    std::string func = "foobar func";
    std::string resultKey = "dummy result";
    int returnValue = 123;

    std::string pyUser = "python user";
    std::string pyFunc = "python func";
    std::string pyEntry = "python entry";

    std::string inputData = "input data";
    std::string outputData = "output data";

    std::string cmdline = "some cmdline args";

    funcCall.set_user(user);
    funcCall.set_function(func);
    funcCall.set_resultkey(resultKey);
    funcCall.set_returnvalue(returnValue);

    funcCall.set_pythonuser(pyUser);
    funcCall.set_pythonfunction(pyFunc);
    funcCall.set_pythonentry(pyEntry);

    funcCall.set_inputdata(inputData.data(), 100);
    funcCall.set_outputdata(outputData.data(), 50);

    funcCall.set_isasync(true);
    funcCall.set_ispython(true);
    funcCall.set_isstatusrequest(true);

    funcCall.set_type(faabric::Message_MessageType_KILL);

    funcCall.set_cmdline(cmdline);

    REQUIRE(funcCall.type() == faabric::Message_MessageType_KILL);
    REQUIRE(user == funcCall.user());
    REQUIRE(func == funcCall.function());
    REQUIRE(resultKey == funcCall.resultkey());
    REQUIRE(returnValue == funcCall.returnvalue());

    // Check serialisation round trip
    std::string serialised = funcCall.SerializeAsString();

    faabric::Message newFuncCall;
    newFuncCall.ParseFromString(serialised);

    REQUIRE(user == newFuncCall.user());
    REQUIRE(func == newFuncCall.function());
    REQUIRE(resultKey == newFuncCall.resultkey());
    REQUIRE(faabric::Message_MessageType_KILL == newFuncCall.type());

    REQUIRE(pyUser == newFuncCall.pythonuser());
    REQUIRE(pyFunc == newFuncCall.pythonfunction());
    REQUIRE(pyEntry == newFuncCall.pythonentry());

    REQUIRE(newFuncCall.isasync());
    REQUIRE(newFuncCall.ispython());
    REQUIRE(newFuncCall.isstatusrequest());

    REQUIRE(cmdline == newFuncCall.cmdline());

    // Check input/ output data
    const std::string actualStrInput = newFuncCall.inputdata();
    const std::string actualStrOutput = newFuncCall.outputdata();

    REQUIRE(inputData == actualStrInput);
    REQUIRE(outputData == actualStrOutput);
}

TEST_CASE("Test protobuf byte handling", "[proto]")
{
    // One message with null terminators, one without
    faabric::Message msgA;
    msgA.set_inputdata("input data A");

    faabric::Message msgB;
    msgA.set_inputdata("input data B");

    std::string serialisedA = msgA.SerializeAsString();
    std::string serialisedB = msgB.SerializeAsString();

    REQUIRE(serialisedA.size() == serialisedB.size());

    faabric::Message newMsgA;
    newMsgA.ParseFromString(serialisedA);

    faabric::Message newMsgB;
    newMsgB.ParseFromString(serialisedB);

    checkMessageEquality(msgA, newMsgA);
    checkMessageEquality(msgB, newMsgB);
}
}
