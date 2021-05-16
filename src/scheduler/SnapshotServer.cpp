#include <faabric/scheduler/SnapshotServer.h>
#include <faabric/snapshot/SnapshotRegistry.h>
#include <faabric/state/State.h>
#include <faabric/util/func.h>
#include <faabric/util/logging.h>

namespace faabric::scheduler {
SnapshotServer::SnapshotServer()
  : faabric::transport::MessageEndpointServer(DEFAULT_SNAPSHOT_HOST,
                                              SNAPSHOT_PORT)
{}

void SnapshotServer::doRecv(faabric::transport::Message header,
                            faabric::transport::Message body)
{
    assert(header.size() == sizeof(int));
    int call = static_cast<int>(*header.data());
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

void SnapshotServer::recvPushSnapshot(faabric::transport::Message msg)
{
    const SnapshotPushRequest* r =
      flatbuffers::GetRoot<SnapshotPushRequest>(msg.udata());
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

void SnapshotServer::recvDeleteSnapshot(faabric::transport::Message msg)
{
    const SnapshotDeleteRequest* r =
      flatbuffers::GetRoot<SnapshotDeleteRequest>(msg.udata());
    faabric::util::getLogger()->info("Deleting shapshot {}", r->key()->c_str());

    faabric::snapshot::SnapshotRegistry& reg =
      faabric::snapshot::getSnapshotRegistry();

    reg.deleteSnapshot(r->key()->str());
}
}
