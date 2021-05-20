#include <faabric/state/DummyStateServer.h>

#include <faabric/state/InMemoryStateKeyValue.h>
#include <faabric/util/logging.h>
#include <faabric/util/network.h>

namespace faabric::state {
DummyStateServer::DummyStateServer()
//: remoteState(faabric::util::getSystemConfig().endpointHost)
  : remoteState(LOCALHOST)
  , stateServer(remoteState)
{}

std::shared_ptr<StateKeyValue> DummyStateServer::getRemoteKv()
{
    if (dummyData.empty()) {
        return remoteState.getKV(dummyUser, dummyKey);
    } else {
        return remoteState.getKV(dummyUser, dummyKey, dummyData.size());
    }
}

std::shared_ptr<StateKeyValue> DummyStateServer::getLocalKv()
{
    if (dummyData.empty()) {
        return state::getGlobalState().getKV(dummyUser, dummyKey);
    } else {
        return state::getGlobalState().getKV(
          dummyUser, dummyKey, dummyData.size());
    }
}

std::vector<uint8_t> DummyStateServer::getRemoteKvValue()
{
    std::vector<uint8_t> actual(dummyData.size(), 0);
    getRemoteKv()->get(actual.data());
    return actual;
}

std::vector<uint8_t> DummyStateServer::getLocalKvValue()
{
    std::vector<uint8_t> actual(dummyData.size(), 0);
    getLocalKv()->get(actual.data());
    return actual;
}

void DummyStateServer::start()
{
    // NOTE - We want to test the server being on a different host.
    // To do this we run the server in a separate thread, forcing it to
    // have a localhost IP, then the main thread is the "client" with a
    // junk IP.
    const std::shared_ptr<spdlog::logger>& logger = faabric::util::getLogger();

    // Override the host endpoint for the server thread. Must be localhost
    // faabric::util::getSystemConfig().endpointHost = LOCALHOST;

    // Master the dummy data in this thread
    if (!dummyData.empty()) {
        const std::shared_ptr<state::StateKeyValue>& kv =
          remoteState.getKV(dummyUser, dummyKey, dummyData.size());
        std::shared_ptr<InMemoryStateKeyValue> inMemKv =
          std::static_pointer_cast<InMemoryStateKeyValue>(kv);

        // Check this kv "thinks" it's master
        if (!inMemKv->isMaster()) {
            logger->error("Dummy state server not master for data");
            throw std::runtime_error("Remote state server failed");
        }

        // Set the data
        kv->set(dummyData.data());
        logger->debug(
          "Finished setting master for test {}/{}", kv->user, kv->key);
    }

    // Start the state server
    // Note - by default the state server runs in a background thread
    logger->debug("Running state server");
    stateServer.start();

    // Give it time to start
    usleep(1000 * 1000);
}

void DummyStateServer::stop()
{
    stateServer.stop();
}
}
