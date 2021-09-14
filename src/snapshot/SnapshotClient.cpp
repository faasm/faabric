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

static std::vector<std::pair<std::string, faabric::util::SnapshotData>>
  snapshotPushes;

static std::vector<
  std::pair<std::string, std::vector<faabric::util::SnapshotDiff>>>
  snapshotDiffPushes;

static std::vector<std::pair<std::string, std::string>> snapshotDeletes;

static std::vector<std::pair<std::string, std::pair<uint32_t, int>>>
  threadResults;

std::vector<std::pair<std::string, faabric::util::SnapshotData>>
getSnapshotPushes()
{
    return snapshotPushes;
}

std::vector<std::pair<std::string, std::vector<faabric::util::SnapshotDiff>>>
getSnapshotDiffPushes()
{
    return snapshotDiffPushes;
}

std::vector<std::pair<std::string, std::string>> getSnapshotDeletes()
{
    return snapshotDeletes;
}

std::vector<std::pair<std::string, std::pair<uint32_t, int>>> getThreadResults()
{
    return threadResults;
}

void clearMockSnapshotRequests()
{
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

void SnapshotClient::pushSnapshot(const std::string& key,
                                  const faabric::util::SnapshotData& data)
{
    if (data.size == 0) {
        SPDLOG_ERROR("Cannot push snapshot {} with size zero to {}", key, host);
        throw std::runtime_error("Pushing snapshot with zero size");
    }

    SPDLOG_DEBUG("Pushing snapshot {} to {} ({} bytes)", key, host, data.size);

    if (faabric::util::isMockMode()) {
        faabric::util::UniqueLock lock(mockMutex);
        snapshotPushes.emplace_back(host, data);
    } else {
        // Set up the main request
        // TODO - avoid copying data here
        flatbuffers::FlatBufferBuilder mb;
        auto keyOffset = mb.CreateString(key);
        auto dataOffset = mb.CreateVector<uint8_t>(data.data, data.size);
        auto requestOffset =
          CreateSnapshotPushRequest(mb, keyOffset, dataOffset);
        mb.Finish(requestOffset);

        // Send it
        SEND_FB_MSG(SnapshotCalls::PushSnapshot, mb)
    }
}

void SnapshotClient::pushSnapshotDiffs(
  std::string snapshotKey,
  std::vector<faabric::util::SnapshotDiff> diffs)
{
    if (faabric::util::isMockMode()) {
        faabric::util::UniqueLock lock(mockMutex);
        snapshotDiffPushes.emplace_back(host, diffs);
    } else {
        SPDLOG_DEBUG("Pushing {} diffs for snapshot {} to {}",
                     diffs.size(),
                     snapshotKey,
                     host);

        flatbuffers::FlatBufferBuilder mb;

        // Create objects for all the chunks
        std::vector<flatbuffers::Offset<SnapshotDiffChunk>> diffsFbVector;
        for (const auto& d : diffs) {
            auto dataOffset = mb.CreateVector<uint8_t>(d.data, d.size);
            auto chunk = CreateSnapshotDiffChunk(mb, d.offset, dataOffset);
            diffsFbVector.push_back(chunk);
        }

        // Set up the main request
        // TODO - avoid copying data here
        auto keyOffset = mb.CreateString(snapshotKey);
        auto diffsOffset = mb.CreateVector(diffsFbVector);
        auto requestOffset =
          CreateSnapshotDiffPushRequest(mb, keyOffset, diffsOffset);
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
