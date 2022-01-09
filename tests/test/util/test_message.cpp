#include <catch2/catch.hpp>

#include <faabric_utils.h>

#include <faabric/util/message.h>

using namespace faabric::util;

namespace tests {
TEST_CASE("Test copying faabric message", "[util]")
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

    msg.set_issgx(true);
    msg.set_sgxsid("test sid string");
    msg.set_sgxnonce("test nonce string");
    msg.set_sgxtag("test tag string");
    msg.set_sgxpolicy("test policy string");
    msg.set_sgxresult("test result string");

    msg.set_recordexecgraph(true);

    msg.set_migrationcheckperiod(33);

    faabric::Message msgCopy;
    copyMessage(&msg, &msgCopy);

    checkMessageEquality(msg, msgCopy);
}
}
