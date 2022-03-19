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
  , reg(faabric::snapshot::getSnapshotRegistry())
{}

void SnapshotServer::doAsyncRecv(int header,
                                 const uint8_t* buffer,
                                 size_t bufferSize)
{
    switch (header) {
        case faabric::snapshot::SnapshotCalls::DeleteSnapshot: {
            recvDeleteSnapshot(buffer, bufferSize);
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
        case faabric::snapshot::SnapshotCalls::PushSnapshotUpdate: {
            return recvPushSnapshotUpdate(buffer, bufferSize);
        }
        case faabric::snapshot::SnapshotCalls::ThreadResult: {
            return recvThreadResult(buffer, bufferSize);
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
                 r->max_size());

    // Set up the snapshot
    size_t snapSize = r->contents()->size();
    std::string snapKey = r->key()->str();
    auto snap = std::make_shared<SnapshotData>(
      std::span((uint8_t*)r->contents()->Data(), snapSize), r->max_size());

    // Add the merge regions
    for (const auto* mr : *r->merge_regions()) {
        snap->addMergeRegion(
          mr->offset(),
          mr->length(),
          static_cast<SnapshotDataType>(mr->data_type()),
          static_cast<SnapshotMergeOperation>(mr->merge_op()));
    }

    // Register snapshot
    reg.registerSnapshot(snapKey, snap);

    snap->clearTrackedChanges();

    // Send response
    return std::make_unique<faabric::EmptyResponse>();
}

std::unique_ptr<google::protobuf::Message> SnapshotServer::recvThreadResult(
  const uint8_t* buffer,
  size_t bufferSize)
{
    const ThreadResultRequest* r =
      flatbuffers::GetRoot<ThreadResultRequest>(buffer);

    SPDLOG_DEBUG("Receiving thread result {} for message {} with {} diffs",
                 r->return_value(),
                 r->message_id(),
                 r->diffs()->size());

    if (r->diffs()->size() > 0) {
        auto snap = reg.getSnapshot(r->key()->str());

        // Convert diffs to snapshot diff objects
        std::vector<SnapshotDiff> diffs;
        diffs.reserve(r->diffs()->size());
        for (const auto* diff : *r->diffs()) {
            diffs.emplace_back(
              static_cast<SnapshotDataType>(diff->data_type()),
              static_cast<SnapshotMergeOperation>(diff->merge_op()),
              diff->offset(),
              std::span<const uint8_t>(diff->data()->data(),
                                       diff->data()->size()));
        }

        // Queue on the snapshot
        snap->queueDiffs(diffs);
    }

    // Set the result locally
    faabric::scheduler::Scheduler& sch = faabric::scheduler::getScheduler();
    sch.setThreadResultLocally(r->message_id(), r->return_value());

    return std::make_unique<faabric::EmptyResponse>();
}

std::unique_ptr<google::protobuf::Message>
SnapshotServer::recvPushSnapshotUpdate(const uint8_t* buffer, size_t bufferSize)
{
    const SnapshotUpdateRequest* r =
      flatbuffers::GetRoot<SnapshotUpdateRequest>(buffer);

    SPDLOG_DEBUG(
      "Queueing {} diffs for snapshot {}", r->diffs()->size(), r->key()->str());

    // Get the snapshot
    auto snap = reg.getSnapshot(r->key()->str());

    // Convert diffs to snapshot diff objects
    std::vector<SnapshotDiff> diffs;
    diffs.reserve(r->diffs()->size());
    for (const auto* diff : *r->diffs()) {
        diffs.emplace_back(
          static_cast<SnapshotDataType>(diff->data_type()),
          static_cast<SnapshotMergeOperation>(diff->merge_op()),
          diff->offset(),
          std::span<const uint8_t>(diff->data()->data(), diff->data()->size()));
    }

    // Queue on the snapshot
    snap->queueDiffs(diffs);

    // Write diffs and set merge regions
    SPDLOG_DEBUG("Writing queued diffs to snapshot {} ({} regions)",
                 r->key()->str(),
                 r->merge_regions()->size());

    // Write queued diffs
    snap->writeQueuedDiffs();

    // Clear merge regions
    snap->clearMergeRegions();

    // Add merge regions from request
    for (const auto* mr : *r->merge_regions()) {
        snap->addMergeRegion(
          mr->offset(),
          mr->length(),
          static_cast<SnapshotDataType>(mr->data_type()),
          static_cast<SnapshotMergeOperation>(mr->merge_op()));
    }

    // Send response
    return std::make_unique<faabric::EmptyResponse>();
}

void SnapshotServer::recvDeleteSnapshot(const uint8_t* buffer,
                                        size_t bufferSize)
{
    const SnapshotDeleteRequest* r =
      flatbuffers::GetRoot<SnapshotDeleteRequest>(buffer);
    SPDLOG_INFO("Deleting shapshot {}", r->key()->c_str());

    // Delete the registry entry
    reg.deleteSnapshot(r->key()->str());
}
}
