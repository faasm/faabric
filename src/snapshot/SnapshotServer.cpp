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

    SPDLOG_DEBUG("Receiving snapshot {} (size {}, max {})",
                 r->key()->c_str(),
                 r->contents()->size(),
                 r->maxSize());

    faabric::snapshot::SnapshotRegistry& reg =
      faabric::snapshot::getSnapshotRegistry();

    // Set up the snapshot
    // Note, the snapshot object must take ownership of the data here, which
    // implies a copy.
    // TODO - can we take ownership from flatbuffer object to avoid the copy?
    size_t snapSize = r->contents()->size();
    std::string snapKey = r->key()->str();
    auto d =
      std::make_shared<faabric::util::SnapshotData>(snapSize, r->maxSize());
    d->copyInData((uint8_t*)r->contents()->Data(), snapSize);

    // Register and make restorable
    d->makeRestorable(snapKey);
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

    // Iterate through the chunks passed in the request
    for (const auto* chunk : *r->chunks()) {
        SPDLOG_TRACE("Applying snapshot diff to {} at {}-{}",
                     r->key()->str(),
                     chunk->offset(),
                     chunk->offset() + chunk->data()->size());

        switch (chunk->dataType()) {
            case (faabric::util::SnapshotDataType::Raw): {
                switch (chunk->mergeOp()) {
                    case (faabric::util::SnapshotMergeOperation::Overwrite): {
                        snap->copyInData((uint8_t*)chunk->data()->data(),
                                         chunk->data()->size(),
                                         chunk->offset());
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
                const auto* diffValue =
                  reinterpret_cast<const int32_t*>(chunk->data()->data());
                const int32_t* original = reinterpret_cast<const int32_t*>(
                  snap->getDataPtr(chunk->offset()));

                int32_t finalValue = 0;
                switch (chunk->mergeOp()) {
                    case (faabric::util::SnapshotMergeOperation::Sum): {
                        finalValue = *original + *diffValue;
                        break;
                    }
                    case (faabric::util::SnapshotMergeOperation::Subtract): {
                        finalValue = *original - *diffValue;
                        break;
                    }
                    case (faabric::util::SnapshotMergeOperation::Product): {
                        finalValue = *original * *diffValue;
                        break;
                    }
                    case (faabric::util::SnapshotMergeOperation::Min): {
                        finalValue = std::min(*original, *diffValue);
                        break;
                    }
                    case (faabric::util::SnapshotMergeOperation::Max): {
                        finalValue = std::max(*original, *diffValue);
                        break;
                    }
                    default: {
                        SPDLOG_ERROR("Unsupported int merge operation: {}",
                                     chunk->mergeOp());
                        throw std::runtime_error(
                          "Unsupported int merge operation");
                    }
                }

                snap->copyInData(
                  BYTES(&finalValue), sizeof(int32_t), chunk->offset());
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
