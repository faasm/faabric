#include <faabric/flat/faabric_generated.h>
#include <faabric/proto/faabric.pb.h>
#include <faabric/snapshot/SnapshotRegistry.h>
#include <faabric/snapshot/SnapshotServer.h>
#include <faabric/state/State.h>
#include <faabric/transport/common.h>
#include <faabric/transport/macros.h>
#include <faabric/util/func.h>
#include <faabric/util/logging.h>
#include <faabric/util/snapshot.h>

#include <sys/mman.h>

namespace faabric::snapshot {
SnapshotServer::SnapshotServer()
  : faabric::transport::MessageEndpointServer(SNAPSHOT_ASYNC_PORT,
                                              SNAPSHOT_SYNC_PORT)
{}

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
    std::memcpy(data.data, r->contents()->Data(), data.size);

    reg.takeSnapshot(r->key()->str(), data, true);

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

    SPDLOG_DEBUG(
      "Applying {} diffs to snapshot {}", r->chunks()->size(), r->key()->str());

    // Get the snapshot
    faabric::snapshot::SnapshotRegistry& reg =
      faabric::snapshot::getSnapshotRegistry();
    faabric::util::SnapshotData& snap = reg.getSnapshot(r->key()->str());

    // Apply diffs to snapshot
    for (const auto* r : *r->chunks()) {
        uint8_t* dest = snap.data + r->offset();
        switch (r->mergeOp()) {
            case (faabric::util::SnapshotMergeOperation::Overwrite): {
                std::memcpy(dest, r->data()->data(), r->data()->size());
                break;
            }
            case (faabric::util::SnapshotMergeOperation::Sum): {
                switch (r->dataType()) {
                    case (faabric::util::SnapshotDataType::Int): {
                        const auto* value =
                          reinterpret_cast<const int32_t*>(r->data()->data());

                        // Add the value
                        *(reinterpret_cast<int32_t*>(dest)) += *value;
                        break;
                    }
                    default: {
                        SPDLOG_ERROR("Unsupported sum data type : {}",
                                     r->dataType());
                        throw std::runtime_error("Unsupported sum data type");
                    }
                }
                break;
            }
            default: {
                SPDLOG_ERROR("Unsupported diff operation: {}", r->mergeOp());
                throw std::runtime_error("Unsupported diff operation");
            }
        }
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

    faabric::snapshot::SnapshotRegistry& reg =
      faabric::snapshot::getSnapshotRegistry();

    // Delete the registry entry
    reg.deleteSnapshot(r->key()->str());
}
}
