#include <catch.hpp>

#include <faabric/transport/MpiMessageEndpoint.h>
#include <faabric_utils.h>

using namespace faabric::transport;

namespace tests {
TEST_CASE_METHOD(MessageContextFixture,
                 "Test send and recv the hosts to rank message",
                 "[transport]")
{
    // Prepare message
    std::vector<std::string> expected = { "foo", "bar" };
    faabric::MpiHostsToRanksMessage sendMsg;
    *sendMsg.mutable_hosts() = { expected.begin(), expected.end() };
    sendMpiHostRankMsg(LOCALHOST, sendMsg);

    // Send message
    faabric::MpiHostsToRanksMessage actual = recvMpiHostRankMsg();

    // Checks
    REQUIRE(actual.hosts().size() == expected.size());
    for (int i = 0; i < actual.hosts().size(); i++) {
        REQUIRE(actual.hosts().Get(i) == expected[i]);
    }
}

TEST_CASE_METHOD(MessageContextFixture,
                 "Test send and recv an MPI message",
                 "[transport]")
{
    std::string thisHost = faabric::util::getSystemConfig().endpointHost;
    MpiMessageEndpoint sendEndpoint(LOCALHOST, 9999, 9998);
    MpiMessageEndpoint recvEndpoint(thisHost, 9998, 9999);

    std::shared_ptr<faabric::MPIMessage> expected =
      std::make_shared<faabric::MPIMessage>();
    expected->set_id(1337);

    sendEndpoint.sendMpiMessage(expected);
    std::shared_ptr<faabric::MPIMessage> actual = recvEndpoint.recvMpiMessage();

    // Checks
    REQUIRE(expected->id() == actual->id());

    REQUIRE_NOTHROW(sendEndpoint.close());
    REQUIRE_NOTHROW(recvEndpoint.close());
}
}
