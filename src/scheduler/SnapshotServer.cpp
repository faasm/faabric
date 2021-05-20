#include <faabric/scheduler/SnapshotServer.h>
#include <faabric/snapshot/SnapshotRegistry.h>
#include <faabric/state/State.h>
#include <faabric/util/func.h>
#include <faabric/util/logging.h>

namespace faabric::scheduler {
SnapshotServer::SnapshotServer()
  : faabric::transport::MessageEndpointServer(SNAPSHOT_PORT)
{}

void SnapshotServer::doRecv(faabric::transport::Message& header,
                            faabric::transport::Message& body)
{
    assert(header.size() == sizeof(uint8_t));
    uint8_t call = static_cast<uint8_t>(*header.data());
    switch (call) {
        case faabric::scheduler::SnapshotCalls::PushSnapshot:
            this->recvPushSnapshot(body);
            break;
        case faabric::scheduler::SnapshotCalls::DeleteSnapshot:
            this->recvDeleteSnapshot(body);
            break;
        default:
            throw std::runtime_error(
              fmt::format("Unrecognized call header: {}", call));
    }
}

void SnapshotServer::recvPushSnapshot(faabric::transport::Message& msg)
{
    // We assume the application to free the underlying message pointer
    msg.persist();

    const SnapshotPushRequest* r =
      flatbuffers::GetRoot<SnapshotPushRequest>(msg.udata());

    flatbuffers::Verifier verifier(msg.udata(), msg.size());
    if (!r->Verify(verifier)) {
        throw std::runtime_error("Error verifying snapshot");
    }

    faabric::util::getLogger()->info("Pushing shapshot {} (size {})",
                                     r->key()->c_str(),
                                     r->contents()->size());

    faabric::snapshot::SnapshotRegistry& reg =
      faabric::snapshot::getSnapshotRegistry();

    // Set up the snapshot
    faabric::util::SnapshotData data;
    data.size = r->contents()->size();
    data.data = r->contents()->Data();
    reg.takeSnapshot(r->key()->str(), data, true);
}

void SnapshotServer::recvDeleteSnapshot(faabric::transport::Message& msg)
{
    const SnapshotDeleteRequest* r =
      flatbuffers::GetRoot<SnapshotDeleteRequest>(msg.udata());
    faabric::util::getLogger()->info("Deleting shapshot {}", r->key()->c_str());

    faabric::snapshot::SnapshotRegistry& reg =
      faabric::snapshot::getSnapshotRegistry();

    // Delete the registry entry
    reg.deleteSnapshot(r->key()->str());
}
}
