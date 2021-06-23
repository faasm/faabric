#include <faabric/flat/faabric_generated.h>
#include <faabric/proto/faabric.pb.h>
#include <faabric/scheduler/SnapshotServer.h>
#include <faabric/snapshot/SnapshotRegistry.h>
#include <faabric/state/State.h>
#include <faabric/transport/common.h>
#include <faabric/transport/macros.h>
#include <faabric/util/func.h>
#include <faabric/util/logging.h>

#include <sys/mman.h>

namespace faabric::scheduler {
SnapshotServer::SnapshotServer()
  : faabric::transport::MessageEndpointServer(SNAPSHOT_PORT)
{}

void SnapshotServer::stop()
{
    // Close the dangling clients
    faabric::scheduler::getScheduler().closeSnapshotClients();

    // Call the parent stop
    MessageEndpointServer::stop();
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

    SPDLOG_DEBUG("Receiving shapshot {} (size {})",
                 r->key()->c_str(),
                 r->contents()->size());

    faabric::snapshot::SnapshotRegistry& reg =
      faabric::snapshot::getSnapshotRegistry();

    // Set up the snapshot
    faabric::util::SnapshotData data;
    data.size = r->contents()->size();

    // TODO - avoid this copy by changing server superclass to allow subclasses
    // to provide a buffer to receive data.
    // TODO - work out snapshot ownership here, how do we know when to delete
    // this data?
    data.data = (uint8_t*)mmap(
      nullptr, data.size, PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    std::memcpy(data.data, r->mutable_contents()->Data(), data.size);

    reg.takeSnapshot(r->key()->str(), data, true);

    // Send response
    faabric::EmptyResponse response;
    SEND_SERVER_RESPONSE(response, r->return_host()->str())
}

void SnapshotServer::recvThreadResult(faabric::transport::Message& msg)
{
    const ThreadResultRequest* r =
      flatbuffers::GetMutableRoot<ThreadResultRequest>(msg.udata());

    // Apply snapshot diffs *first* (these must be applied before other threads
    // can continue)
    if (r->chunks() != nullptr && r->chunks()->size() > 0) {
        applyDiffsToSnapshot(r->key()->str(), r->chunks());
    }

    SPDLOG_DEBUG("Receiving thread result {} for message {}",
                 r->return_value(),
                 r->message_id());

    faabric::scheduler::Scheduler& sch = faabric::scheduler::getScheduler();
    sch.setThreadResultLocally(r->message_id(), r->return_value());
}

void SnapshotServer::recvPushSnapshotDiffs(faabric::transport::Message& msg)
{
    const SnapshotDiffPushRequest* r =
      flatbuffers::GetMutableRoot<SnapshotDiffPushRequest>(msg.udata());

    applyDiffsToSnapshot(r->key()->str(), r->chunks());

    // Send response
    faabric::EmptyResponse response;
    SEND_SERVER_RESPONSE(response, r->return_host()->str())
}

void SnapshotServer::applyDiffsToSnapshot(
  const std::string& snapshotKey,
  const flatbuffers::Vector<flatbuffers::Offset<SnapshotDiffChunk>>* diffs)
{
    SPDLOG_DEBUG(
      "Applying {} diffs to snapshot {}", diffs->size(), snapshotKey);

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
    SPDLOG_INFO("Deleting shapshot {}", r->key()->c_str());

    faabric::snapshot::SnapshotRegistry& reg =
      faabric::snapshot::getSnapshotRegistry();

    // Delete the registry entry
    reg.deleteSnapshot(r->key()->str());
}
}
