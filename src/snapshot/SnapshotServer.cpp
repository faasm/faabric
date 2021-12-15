#include <faabric/flat/faabric_generated.h>
#include <faabric/proto/faabric.pb.h>
#include <faabric/snapshot/SnapshotRegistry.h>
#include <faabric/snapshot/SnapshotServer.h>
#include <faabric/state/State.h>
#include <faabric/transport/PointToPointBroker.h>
#include <faabric/transport/common.h>
#include <faabric/transport/macros.h>
#include <faabric/util/bytes.h>
#include <faabric/util/func.h>
#include <faabric/util/logging.h>
#include <faabric/util/memory.h>
#include <faabric/util/snapshot.h>

using namespace faabric::util;

namespace faabric::snapshot {
SnapshotServer::SnapshotServer()
  : faabric::transport::MessageEndpointServer(
      SNAPSHOT_ASYNC_PORT,
      SNAPSHOT_SYNC_PORT,
      SNAPSHOT_INPROC_LABEL,
      getSystemConfig().snapshotServerThreads)
  , broker(faabric::transport::getPointToPointBroker())
{}

size_t SnapshotServer::diffsApplied() const
{
    return diffsAppliedCounter.load(std::memory_order_acquire);
}

void SnapshotServer::doAsyncRecv(int header,
                                 const uint8_t* buffer,
                                 size_t bufferSize)
{
    switch (header) {
        case faabric::snapshot::SnapshotCalls::DeleteSnapshot: {
            this->recvDeleteSnapshot(buffer, bufferSize);
            break;
        }
        case faabric::snapshot::SnapshotCalls::ThreadResult: {
            this->recvThreadResult(buffer, bufferSize);
            break;
        }
        default: {
            throw std::runtime_error(
              fmt::format("Unrecognized async call header: {}", header));
        }
    }
}

std::unique_ptr<google::protobuf::Message>
SnapshotServer::doSyncRecv(int header, const uint8_t* buffer, size_t bufferSize)
{
    switch (header) {
        case faabric::snapshot::SnapshotCalls::PushSnapshot: {
            return recvPushSnapshot(buffer, bufferSize);
        }
        case faabric::snapshot::SnapshotCalls::PushSnapshotDiffs: {
            return recvPushSnapshotDiffs(buffer, bufferSize);
        }
        default: {
            throw std::runtime_error(
              fmt::format("Unrecognized sync call header: {}", header));
        }
    }
}

std::unique_ptr<google::protobuf::Message> SnapshotServer::recvPushSnapshot(
  const uint8_t* buffer,
  size_t bufferSize)
{
    const SnapshotPushRequest* r =
      flatbuffers::GetRoot<SnapshotPushRequest>(buffer);

    if (r->contents()->size() == 0) {
        SPDLOG_ERROR("Received shapshot {} with zero size", r->key()->c_str());
        throw std::runtime_error("Received snapshot with zero size");
    }

    SPDLOG_DEBUG("Receiving snapshot {} (size {}, max {})",
                 r->key()->c_str(),
                 r->contents()->size(),
                 r->maxSize());

    faabric::snapshot::SnapshotRegistry& reg =
      faabric::snapshot::getSnapshotRegistry();

    // Set up the snapshot
    size_t snapSize = r->contents()->size();
    std::string snapKey = r->key()->str();
    auto d = std::make_shared<SnapshotData>(
      std::span((uint8_t*)r->contents()->Data(), snapSize), r->maxSize());

    // Register snapshot
    reg.registerSnapshot(snapKey, d);

    // Send response
    return std::make_unique<faabric::EmptyResponse>();
}

void SnapshotServer::recvThreadResult(const uint8_t* buffer, size_t bufferSize)
{
    const ThreadResultRequest* r =
      flatbuffers::GetRoot<ThreadResultRequest>(buffer);

    SPDLOG_DEBUG("Receiving thread result {} for message {}",
                 r->return_value(),
                 r->message_id());

    faabric::scheduler::Scheduler& sch = faabric::scheduler::getScheduler();
    sch.setThreadResultLocally(r->message_id(), r->return_value());
}

std::unique_ptr<google::protobuf::Message>
SnapshotServer::recvPushSnapshotDiffs(const uint8_t* buffer, size_t bufferSize)
{
    const SnapshotDiffPushRequest* r =
      flatbuffers::GetRoot<SnapshotDiffPushRequest>(buffer);
    int groupId = r->groupid();

    SPDLOG_DEBUG(
      "Applying {} diffs to snapshot {}", r->chunks()->size(), r->key()->str());

    // Get the snapshot
    faabric::snapshot::SnapshotRegistry& reg =
      faabric::snapshot::getSnapshotRegistry();
    auto snap = reg.getSnapshot(r->key()->str());

    // Lock the function group if it exists
    if (groupId > 0 &&
        faabric::transport::PointToPointGroup::groupExists(groupId)) {
        faabric::transport::PointToPointGroup::getGroup(r->groupid())
          ->localLock();
    }

    // Convert chunks to snapshot diff objects
    std::vector<SnapshotDiff> diffs;
    for (const auto* chunk : *r->chunks()) {
        diffs.emplace_back(
          static_cast<SnapshotDataType>(chunk->dataType()),
          static_cast<SnapshotMergeOperation>(chunk->mergeOp()),
          chunk->offset(),
          chunk->data()->data(),
          chunk->data()->size());
    }
    snap->writeDiffs(diffs);

    // Make changes visible to other threads
    std::atomic_thread_fence(std::memory_order_release);
    this->diffsAppliedCounter.fetch_add(diffs.size(),
                                        std::memory_order_acq_rel);

    // Unlock group if exists
    if (groupId > 0 &&
        faabric::transport::PointToPointGroup::groupExists(groupId)) {
        faabric::transport::PointToPointGroup::getGroup(r->groupid())
          ->localUnlock();
    }

    // Reset dirty tracking having applied diffs
    SPDLOG_DEBUG("Resetting dirty page tracking having applied diffs to {}",
                 r->key()->str());

    // Send response
    return std::make_unique<faabric::EmptyResponse>();
}

void SnapshotServer::recvDeleteSnapshot(const uint8_t* buffer,
                                        size_t bufferSize)
{
    const SnapshotDeleteRequest* r =
      flatbuffers::GetRoot<SnapshotDeleteRequest>(buffer);
    SPDLOG_INFO("Deleting shapshot {}", r->key()->c_str());

    faabric::snapshot::SnapshotRegistry& reg =
      faabric::snapshot::getSnapshotRegistry();

    // Delete the registry entry
    reg.deleteSnapshot(r->key()->str());
}
}
