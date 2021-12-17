#include <faabric/snapshot/SnapshotClient.h>
#include <faabric/transport/common.h>
#include <faabric/transport/macros.h>
#include <faabric/util/config.h>
#include <faabric/util/logging.h>
#include <faabric/util/queue.h>
#include <faabric/util/testing.h>

namespace faabric::snapshot {

// -----------------------------------
// Mocking
// -----------------------------------

static std::mutex mockMutex;

static std::vector<
  std::pair<std::string, std::shared_ptr<faabric::util::SnapshotData>>>
  snapshotPushes;

static std::vector<
  std::pair<std::string, std::vector<faabric::util::SnapshotDiff>>>
  snapshotDiffPushes;

static std::vector<std::pair<std::string, std::string>> snapshotDeletes;

static std::vector<std::pair<std::string, std::pair<uint32_t, int>>>
  threadResults;

std::vector<
  std::pair<std::string, std::shared_ptr<faabric::util::SnapshotData>>>
getSnapshotPushes()
{
    faabric::util::UniqueLock lock(mockMutex);
    return snapshotPushes;
}

std::vector<std::pair<std::string, std::vector<faabric::util::SnapshotDiff>>>
getSnapshotDiffPushes()
{
    faabric::util::UniqueLock lock(mockMutex);
    return snapshotDiffPushes;
}

std::vector<std::pair<std::string, std::string>> getSnapshotDeletes()
{
    faabric::util::UniqueLock lock(mockMutex);
    return snapshotDeletes;
}

std::vector<std::pair<std::string, std::pair<uint32_t, int>>> getThreadResults()
{
    faabric::util::UniqueLock lock(mockMutex);
    return threadResults;
}

void clearMockSnapshotRequests()
{
    faabric::util::UniqueLock lock(mockMutex);
    snapshotPushes.clear();
    snapshotDiffPushes.clear();
    snapshotDeletes.clear();
    threadResults.clear();
}

// -----------------------------------
// Snapshot client
// -----------------------------------

SnapshotClient::SnapshotClient(const std::string& hostIn)
  : faabric::transport::MessageEndpointClient(hostIn,
                                              SNAPSHOT_ASYNC_PORT,
                                              SNAPSHOT_SYNC_PORT)
{}

void SnapshotClient::pushSnapshot(
  const std::string& key,
  int groupId,
  std::shared_ptr<faabric::util::SnapshotData> data)
{
    if (data->getSize() == 0) {
        SPDLOG_ERROR("Cannot push snapshot {} with size zero to {}", key, host);
        throw std::runtime_error("Pushing snapshot with zero size");
    }

    SPDLOG_DEBUG(
      "Pushing snapshot {} to {} ({} bytes)", key, host, data->getSize());

    if (faabric::util::isMockMode()) {
        faabric::util::UniqueLock lock(mockMutex);

        snapshotPushes.emplace_back(host, data);
    } else {
        // Set up the main request
        // TODO - avoid copying data here?
        flatbuffers::FlatBufferBuilder mb;
        auto keyOffset = mb.CreateString(key);
        auto dataOffset =
          mb.CreateVector<uint8_t>(data->getDataPtr(), data->getSize());
        auto requestOffset = CreateSnapshotPushRequest(
          mb, keyOffset, groupId, data->getMaxSize(), dataOffset);
        mb.Finish(requestOffset);

        // Send it
        SEND_FB_MSG(SnapshotCalls::PushSnapshot, mb)
    }
}

void SnapshotClient::pushSnapshotDiffs(
  std::string snapshotKey,
  int groupId,
  bool force,
  const std::vector<faabric::util::SnapshotDiff>& diffs)
{
    if (faabric::util::isMockMode()) {
        faabric::util::UniqueLock lock(mockMutex);
        snapshotDiffPushes.emplace_back(host, diffs);
    } else {
        SPDLOG_DEBUG("Pushing {} diffs for snapshot {} to {} (group {})",
                     diffs.size(),
                     snapshotKey,
                     host,
                     groupId);

        flatbuffers::FlatBufferBuilder mb;

        // Create objects for all the chunks
        std::vector<flatbuffers::Offset<SnapshotDiffChunk>> diffsFbVector;
        for (const auto& d : diffs) {
            std::span<const uint8_t> diffData = d.getData();
            auto dataOffset =
              mb.CreateVector<uint8_t>(diffData.data(), diffData.size());

            auto chunk = CreateSnapshotDiffChunk(
              mb, d.getOffset(), d.getDataType(), d.getOperation(), dataOffset);
            diffsFbVector.push_back(chunk);
        }

        // Set up the request
        auto keyOffset = mb.CreateString(snapshotKey);
        auto diffsOffset = mb.CreateVector(diffsFbVector);
        auto requestOffset = CreateSnapshotDiffPushRequest(
          mb, keyOffset, groupId, force, diffsOffset);
        mb.Finish(requestOffset);

        SEND_FB_MSG(SnapshotCalls::PushSnapshotDiffs, mb);
    }
}

void SnapshotClient::deleteSnapshot(const std::string& key)
{
    if (faabric::util::isMockMode()) {
        faabric::util::UniqueLock lock(mockMutex);
        snapshotDeletes.emplace_back(host, key);

    } else {
        SPDLOG_DEBUG("Deleting snapshot {} from {}", key, host);

        // TODO - avoid copying data here
        flatbuffers::FlatBufferBuilder mb;
        auto keyOffset = mb.CreateString(key);
        auto requestOffset = CreateSnapshotDeleteRequest(mb, keyOffset);
        mb.Finish(requestOffset);

        SEND_FB_MSG_ASYNC(SnapshotCalls::DeleteSnapshot, mb);
    }
}

void SnapshotClient::pushThreadResult(uint32_t messageId, int returnValue)
{
    if (faabric::util::isMockMode()) {
        faabric::util::UniqueLock lock(mockMutex);
        threadResults.emplace_back(
          std::make_pair(host, std::make_pair(messageId, returnValue)));

    } else {
        flatbuffers::FlatBufferBuilder mb;
        flatbuffers::Offset<ThreadResultRequest> requestOffset;

        SPDLOG_DEBUG("Sending thread result for {} to {}", messageId, host);

        // Create message without diffs
        requestOffset = CreateThreadResultRequest(mb, messageId, returnValue);

        mb.Finish(requestOffset);
        SEND_FB_MSG_ASYNC(SnapshotCalls::ThreadResult, mb)
    }
}
}
