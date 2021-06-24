#include <faabric/scheduler/SnapshotClient.h>
#include <faabric/transport/common.h>
#include <faabric/util/config.h>
#include <faabric/util/logging.h>
#include <faabric/util/queue.h>
#include <faabric/util/testing.h>

namespace faabric::scheduler {

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

static std::vector<
  std::pair<std::string,
            std::tuple<uint32_t,
                       int,
                       std::string,
                       std::vector<faabric::util::SnapshotDiff>>>>
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

std::vector<std::pair<std::string,
                      std::tuple<uint32_t,
                                 int,
                                 std::string,
                                 std::vector<faabric::util::SnapshotDiff>>>>
getThreadResults()
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
  : faabric::transport::MessageEndpointClient(hostIn, SNAPSHOT_PORT)
{}

void SnapshotClient::pushSnapshot(const std::string& key,
                                  const faabric::util::SnapshotData& data)
{
    SPDLOG_DEBUG("Pushing snapshot {} to {}", key, host);

    if (faabric::util::isMockMode()) {
        faabric::util::UniqueLock lock(mockMutex);
        snapshotPushes.emplace_back(host, data);
    } else {
        const faabric::util::SystemConfig& conf =
          faabric::util::getSystemConfig();

        // Set up the main request
        // TODO - avoid copying data here
        flatbuffers::FlatBufferBuilder mb;
        auto returnHostOffset = mb.CreateString(conf.endpointHost);
        auto keyOffset = mb.CreateString(key);
        auto dataOffset = mb.CreateVector<uint8_t>(data.data, data.size);
        auto requestOffset = CreateSnapshotPushRequest(
          mb, returnHostOffset, keyOffset, dataOffset);

        // Send it
        mb.Finish(requestOffset);
        uint8_t* buffer = mb.GetBufferPointer();
        int size = mb.GetSize();
        faabric::EmptyResponse response;
        syncSend(faabric::scheduler::SnapshotCalls::PushSnapshot,
                 buffer,
                 size,
                 &response);
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

        const faabric::util::SystemConfig& conf =
          faabric::util::getSystemConfig();

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
        auto returnHostOffset = mb.CreateString(conf.endpointHost);
        auto keyOffset = mb.CreateString(snapshotKey);
        auto diffsOffset = mb.CreateVector(diffsFbVector);
        auto requestOffset = CreateSnapshotDiffPushRequest(
          mb, returnHostOffset, keyOffset, diffsOffset);

        mb.Finish(requestOffset);
        uint8_t* buffer = mb.GetBufferPointer();
        int size = mb.GetSize();
        faabric::EmptyResponse response;
        syncSend(faabric::scheduler::SnapshotCalls::PushSnapshotDiffs,
                 buffer,
                 size,
                 &response);
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
        uint8_t* buffer = mb.GetBufferPointer();
        int size = mb.GetSize();
        asyncSend(
          faabric::scheduler::SnapshotCalls::PushSnapshotDiffs, buffer, size);
    }
}

void SnapshotClient::pushThreadResult(uint32_t messageId, int returnValue)
{
    std::vector<faabric::util::SnapshotDiff> empty;
    pushThreadResult(messageId, returnValue, "", empty);
}

void SnapshotClient::pushThreadResult(
  uint32_t messageId,
  int returnValue,
  const std::string& snapshotKey,
  const std::vector<faabric::util::SnapshotDiff>& diffs)
{
    if (faabric::util::isMockMode()) {
        faabric::util::UniqueLock lock(mockMutex);
        threadResults.emplace_back(std::make_pair(
          host, std::make_tuple(messageId, returnValue, snapshotKey, diffs)));

    } else {
        flatbuffers::FlatBufferBuilder mb;
        flatbuffers::Offset<ThreadResultRequest> requestOffset;

        if (!diffs.empty()) {
            SPDLOG_DEBUG(
              "Sending thread result for {} to {} (plus {} snapshot diffs)",
              messageId,
              host,
              diffs.size());

            // Create objects for the diffs
            std::vector<flatbuffers::Offset<SnapshotDiffChunk>> diffsFbVector;
            for (const auto& d : diffs) {
                auto dataOffset = mb.CreateVector<uint8_t>(d.data, d.size);
                auto chunk = CreateSnapshotDiffChunk(mb, d.offset, dataOffset);
                diffsFbVector.push_back(chunk);
            }

            // Create message with diffs
            auto diffsOffset = mb.CreateVector(diffsFbVector);

            auto keyOffset = mb.CreateString(snapshotKey);
            requestOffset = CreateThreadResultRequest(
              mb, messageId, returnValue, keyOffset, diffsOffset);
        } else {
            SPDLOG_DEBUG(
              "Sending thread result for {} to {} (with no snapshot diffs)",
              messageId,
              host);

            // Create message without diffs
            requestOffset =
              CreateThreadResultRequest(mb, messageId, returnValue);
        }

        mb.Finish(requestOffset);
        uint8_t* buffer = mb.GetBufferPointer();
        int size = mb.GetSize();
        asyncSend(
          faabric::scheduler::SnapshotCalls::PushSnapshotDiffs, buffer, size);
    }
}
}
