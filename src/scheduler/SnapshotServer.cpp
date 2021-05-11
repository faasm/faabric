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

void SnapshotServer::doRecv(const void* headerData,
                            int headerSize,
                            const void* bodyData,
                            int bodySize)
{
    int call = static_cast<int>(*static_cast<const char*>(headerData));
    switch (call) {
        case faabric::scheduler::SnapshotCalls::PushSnapshot:
            this->recvPushSnapshot(bodyData, bodySize);
            break;
        case faabric::scheduler::SnapshotCalls::DeleteSnapshot:
            this->recvDeleteSnapshot(bodyData, bodySize);
            break;
        default:
            throw std::runtime_error(
              fmt::format("Unrecognized call header: {}", call));
    }
}

void SnapshotServer::recvPushSnapshot(const void* msgData, int size)
{
    // const SnapshotPushRequest* r = request->GetRoot();
    const SnapshotPushRequest* r =
      flatbuffers::GetRoot<SnapshotPushRequest>(msgData);
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

void SnapshotServer::recvDeleteSnapshot(const void* msgData, int size)
{
    const SnapshotDeleteRequest* r =
      flatbuffers::GetRoot<SnapshotDeleteRequest>(msgData);
    faabric::util::getLogger()->info("Deleting shapshot {}", r->key()->c_str());

    faabric::snapshot::SnapshotRegistry& reg =
      faabric::snapshot::getSnapshotRegistry();

    reg.deleteSnapshot(r->key()->str());
}
}
