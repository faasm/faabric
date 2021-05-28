#include <faabric/flat/faabric_generated.h>
#include <faabric/proto/faabric.pb.h>
#include <faabric/scheduler/SnapshotServer.h>
#include <faabric/snapshot/SnapshotRegistry.h>
#include <faabric/state/State.h>
#include <faabric/transport/macros.h>
#include <faabric/util/func.h>
#include <faabric/util/logging.h>

namespace faabric::scheduler {
SnapshotServer::SnapshotServer()
  : faabric::transport::MessageEndpointServer(SNAPSHOT_PORT)
{}

void SnapshotServer::stop()
{
    // Close the dangling clients
    faabric::scheduler::getScheduler().closeSnapshotClients();

    // Call the parent stop
    MessageEndpointServer::stop(faabric::transport::getGlobalMessageContext());
}

void SnapshotServer::doRecv(faabric::transport::Message& header,
                            faabric::transport::Message& body)
{
    assert(header.size() == sizeof(uint8_t));
    uint8_t call = static_cast<uint8_t>(*header.data());
    switch (call) {
        case faabric::scheduler::SnapshotCalls::PushSnapshot:
            this->recvPushSnapshot(body);
            break;
        case faabric::scheduler::SnapshotCalls::PushSnapshotDiffs:
            this->recvPushSnapshotDiffs(body);
            break;
        case faabric::scheduler::SnapshotCalls::DeleteSnapshot:
            this->recvDeleteSnapshot(body);
            break;
        case faabric::scheduler::SnapshotCalls::ThreadResult:
            this->recvThreadResult(body);
            break;
        default:
            throw std::runtime_error(
              fmt::format("Unrecognized call header: {}", call));
    }
}

void SnapshotServer::recvPushSnapshot(faabric::transport::Message& msg)
{
    SnapshotPushRequest* r =
      flatbuffers::GetMutableRoot<SnapshotPushRequest>(msg.udata());

    faabric::util::getLogger()->debug("Receiving shapshot {} (size {})",
                                      r->key()->c_str(),
                                      r->contents()->size());

    faabric::snapshot::SnapshotRegistry& reg =
      faabric::snapshot::getSnapshotRegistry();

    // Set up the snapshot
    faabric::util::SnapshotData data;
    data.size = r->contents()->size();
    data.data = r->mutable_contents()->Data();
    reg.takeSnapshot(r->key()->str(), data, true);

    // Note that now the snapshot data is owned by Faabric and will be deleted
    // later, so we don't want the message to delete it
    msg.persist();

    // Send response
    faabric::EmptyResponse response;
    SEND_SERVER_RESPONSE(response, r->return_host()->str(), SNAPSHOT_PORT)
}

void SnapshotServer::recvThreadResult(faabric::transport::Message& msg)
{
    const ThreadResultRequest* r =
      flatbuffers::GetMutableRoot<ThreadResultRequest>(msg.udata());

    // Apply snapshot diffs *first* (these must be applied before other threads
    // can continue)
    if (r->chunks()->size() > 0) {
        faabric::util::getLogger()->debug("Receiving {} diffs to snapshot {}",
                                          r->chunks()->size(),
                                          r->key()->c_str());

        applyDiffsToSnapshot(r->key()->str(), r->chunks());
    }

    faabric::util::getLogger()->debug(
      "Receiving thread result {} for message {}",
      r->message_id(),
      r->return_value());

    faabric::scheduler::Scheduler& sch = faabric::scheduler::getScheduler();
    sch.setThreadResultLocally(r->message_id(), r->return_value());
}

void SnapshotServer::recvPushSnapshotDiffs(faabric::transport::Message& msg)
{
    const SnapshotDiffPushRequest* r =
      flatbuffers::GetMutableRoot<SnapshotDiffPushRequest>(msg.udata());

    faabric::util::getLogger()->debug("Receiving {} diffs to snapshot {}",
                                      r->chunks()->size(),
                                      r->key()->c_str());

    applyDiffsToSnapshot(r->key()->str(), r->chunks());

    // Send response
    faabric::EmptyResponse response;
    SEND_SERVER_RESPONSE(response, r->return_host()->str(), SNAPSHOT_PORT)
}

void SnapshotServer::applyDiffsToSnapshot(
  const std::string& snapshotKey,
  const flatbuffers::Vector<flatbuffers::Offset<SnapshotDiffChunk>>* diffs)
{
    // Get the snapshot
    faabric::snapshot::SnapshotRegistry& reg =
      faabric::snapshot::getSnapshotRegistry();

    faabric::util::SnapshotData& snap = reg.getSnapshot(snapshotKey);

    // Copy diffs to snapshot
    for (const auto* r : *diffs) {
        const uint8_t* chunkPtr = r->data()->data();
        uint8_t* dest = snap.data + r->offset();
        std::memcpy(dest, chunkPtr, r->data()->size());
    }
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
