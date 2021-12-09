#include <faabric/flat/faabric_generated.h>
#include <faabric/proto/faabric.pb.h>
#include <faabric/snapshot/SnapshotRegistry.h>
#include <faabric/snapshot/SnapshotServer.h>
#include <faabric/state/State.h>
#include <faabric/transport/PointToPointBroker.h>
#include <faabric/transport/common.h>
#include <faabric/transport/macros.h>
#include <faabric/util/func.h>
#include <faabric/util/logging.h>
#include <faabric/util/memory.h>
#include <faabric/util/snapshot.h>

namespace faabric::snapshot {
SnapshotServer::SnapshotServer()
  : faabric::transport::MessageEndpointServer(
      SNAPSHOT_ASYNC_PORT,
      SNAPSHOT_SYNC_PORT,
      SNAPSHOT_INPROC_LABEL,
      faabric::util::getSystemConfig().snapshotServerThreads)
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

    SPDLOG_DEBUG("Receiving snapshot {} (size {})",
                 r->key()->c_str(),
                 r->contents()->size());

    faabric::snapshot::SnapshotRegistry& reg =
      faabric::snapshot::getSnapshotRegistry();

    // Set up the snapshot
    faabric::util::SnapshotData snap;
    snap.size = r->contents()->size();

    if (r->maxSize() > 0) {
        // Allocate virtual memory big enough for the max size if provided
        snap.data = faabric::util::allocateVirtualMemory(r->maxSize());

        // Claim just the snapshot region
        faabric::util::claimVirtualMemory(snap.data, snap.size);
    } else {
        // Normal provisioning of just snapshot region
        snap.data = faabric::util::allocatePrivateMemory(snap.size);
    }

    // TODO - somehow avoid this copy?
    std::memcpy(snap.data, r->contents()->Data(), snap.size);

    // TODO - avoid a further copy in here when setting up fd
    reg.takeSnapshot(r->key()->str(), snap, true);

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

    // Work out max size of the snapshot and extend if necessary
    uint32_t maxSize = 0;
    for (const auto* chunk : *r->chunks()) {
        maxSize =
          std::max<uint32_t>(chunk->offset() + chunk->data()->size(), maxSize);
    }

    if (maxSize > snap->size) {
        SPDLOG_DEBUG("Expanding snapshot {} to accommodate diffs ({} > {})",
                     r->key()->str(),
                     maxSize,
                     snap->size);

        reg.changeSnapshotSize(r->key()->str(), maxSize);
    }

    // Iterate through the chunks passed in the request
    for (const auto* chunk : *r->chunks()) {
        uint8_t* dest = snap->data + chunk->offset();

        SPDLOG_TRACE("Applying snapshot diff to {} at {}-{}",
                     r->key()->str(),
                     chunk->offset(),
                     chunk->offset() + chunk->data()->size());

        switch (chunk->dataType()) {
            case (faabric::util::SnapshotDataType::Raw): {
                switch (chunk->mergeOp()) {
                    case (faabric::util::SnapshotMergeOperation::Overwrite): {
                        std::memcpy(
                          dest, chunk->data()->data(), chunk->data()->size());
                        break;
                    }
                    default: {
                        SPDLOG_ERROR("Unsupported raw merge operation: {}",
                                     chunk->mergeOp());
                        throw std::runtime_error(
                          "Unsupported raw merge operation");
                    }
                }
                break;
            }
            case (faabric::util::SnapshotDataType::Int): {
                const auto* value =
                  reinterpret_cast<const int32_t*>(chunk->data()->data());
                auto* destValue = reinterpret_cast<int32_t*>(dest);
                switch (chunk->mergeOp()) {
                    case (faabric::util::SnapshotMergeOperation::Sum): {
                        *destValue += *value;
                        break;
                    }
                    case (faabric::util::SnapshotMergeOperation::Subtract): {
                        *destValue -= *value;
                        break;
                    }
                    case (faabric::util::SnapshotMergeOperation::Product): {
                        *destValue *= *value;
                        break;
                    }
                    case (faabric::util::SnapshotMergeOperation::Min): {
                        *destValue = std::min(*destValue, *value);
                        break;
                    }
                    case (faabric::util::SnapshotMergeOperation::Max): {
                        *destValue = std::max(*destValue, *value);
                        break;
                    }
                    default: {
                        SPDLOG_ERROR("Unsupported int merge operation: {}",
                                     chunk->mergeOp());
                        throw std::runtime_error(
                          "Unsupported int merge operation");
                    }
                }
                break;
            }
            default: {
                SPDLOG_ERROR("Unsupported data type: {}", chunk->dataType());
                throw std::runtime_error("Unsupported merge data type");
            }
        }

        // Make changes visible to other threads
        std::atomic_thread_fence(std::memory_order_release);
        this->diffsAppliedCounter.fetch_add(1, std::memory_order_acq_rel);
    }

    // Update snapshot
    if (maxSize > snap->size) {
        snap->updateFd();
    }

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
