#include <catch2/catch.hpp>

#include <faabric/transport/MpiMessageEndpoint.h>
#include <faabric_utils.h>

using namespace faabric::transport;

namespace tests {

TEST_CASE_METHOD(SchedulerTestFixture,
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

    REQUIRE(expected->id() == actual->id());
}
}
