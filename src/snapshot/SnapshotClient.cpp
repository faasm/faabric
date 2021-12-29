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

        std::vector<flatbuffers::Offset<SnapshotMergeRegionRequest>>
          mrsFbVector;
        mrsFbVector.reserve(data->getMergeRegions().size());
        for (const auto& m : data->getMergeRegions()) {
            auto mr = CreateSnapshotMergeRegionRequest(mb,
                                                       m.second.offset,
                                                       m.second.length,
                                                       m.second.dataType,
                                                       m.second.operation);
            mrsFbVector.push_back(mr);
        }

        auto keyOffset = mb.CreateString(key);
        auto dataOffset =
          mb.CreateVector<uint8_t>(data->getDataPtr(), data->getSize());
        auto mrsOffset = mb.CreateVector(mrsFbVector);
        auto requestOffset = CreateSnapshotPushRequest(
          mb, keyOffset, data->getMaxSize(), dataOffset, mrsOffset);
        mb.Finish(requestOffset);

        // Send it
        SEND_FB_MSG(SnapshotCalls::PushSnapshot, mb)
    }
}

void SnapshotClient::pushSnapshotUpdate(
  std::string snapshotKey,
  const std::shared_ptr<faabric::util::SnapshotData>& data,
  const std::vector<faabric::util::SnapshotDiff>& diffs)
{
    SPDLOG_DEBUG("Pushing update to snapshot {} to {} ({} diffs, {} regions)",
                 snapshotKey,
                 host,
                 diffs.size(),
                 data->getMergeRegions().size());

    doPushSnapshotDiffs(snapshotKey, data, diffs);
}

void SnapshotClient::pushSnapshotDiffs(
  std::string snapshotKey,
  const std::vector<faabric::util::SnapshotDiff>& diffs)
{
    SPDLOG_DEBUG("Pushing {} diffs for snapshot {} to {}",
                 diffs.size(),
                 snapshotKey,
                 host);

    doPushSnapshotDiffs(snapshotKey, nullptr, diffs);
}

void SnapshotClient::doPushSnapshotDiffs(
  const std::string& snapshotKey,
  const std::shared_ptr<faabric::util::SnapshotData>& data,
  const std::vector<faabric::util::SnapshotDiff>& diffs)
{
    if (faabric::util::isMockMode()) {
        faabric::util::UniqueLock lock(mockMutex);
        snapshotDiffPushes.emplace_back(host, diffs);
    } else {
        flatbuffers::FlatBufferBuilder mb;

        // Create objects for all the diffs
        std::vector<flatbuffers::Offset<SnapshotDiffRequest>> diffsFbVector;
        diffsFbVector.reserve(diffs.size());
        for (const auto& d : diffs) {
            std::span<const uint8_t> diffData = d.getData();
            auto dataOffset =
              mb.CreateVector<uint8_t>(diffData.data(), diffData.size());

            auto diff = CreateSnapshotDiffRequest(
              mb, d.getOffset(), d.getDataType(), d.getOperation(), dataOffset);
            diffsFbVector.push_back(diff);
        }

        // If we have snapshot data, we need to include the merge regions and
        // force too.
        std::vector<flatbuffers::Offset<SnapshotMergeRegionRequest>>
          mrsFbVector;
        bool force = false;
        if (data != nullptr) {
            mrsFbVector.reserve(data->getMergeRegions().size());
            for (const auto& m : data->getMergeRegions()) {
                auto mr = CreateSnapshotMergeRegionRequest(mb,
                                                           m.second.offset,
                                                           m.second.length,
                                                           m.second.dataType,
                                                           m.second.operation);
                mrsFbVector.push_back(mr);
            }

            force = true;
        } else {
            force = false;
        }

        auto keyOffset = mb.CreateString(snapshotKey);
        auto diffsOffset = mb.CreateVector(diffsFbVector);
        auto mrsOffset = mb.CreateVector(mrsFbVector);

        auto requestOffset = CreateSnapshotDiffPushRequest(
          mb, keyOffset, force, mrsOffset, diffsOffset);

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
