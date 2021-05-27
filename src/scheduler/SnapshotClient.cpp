#include <faabric/scheduler/SnapshotClient.h>
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
{
    this->open(faabric::transport::getGlobalMessageContext());
}

void SnapshotClient::sendHeader(faabric::scheduler::SnapshotCalls call)
{
    uint8_t header = static_cast<uint8_t>(call);
    send(&header, sizeof(header), true);
}

void SnapshotClient::pushSnapshot(const std::string& key,
                                  const faabric::util::SnapshotData& data)
{
    const auto& logger = faabric::util::getLogger();
    logger->debug("Pushing snapshot {} to {}", key, host);

    if (faabric::util::isMockMode()) {
        faabric::util::UniqueLock lock(mockMutex);
        snapshotPushes.emplace_back(host, data);
    } else {
        // Send the header first
        sendHeader(faabric::scheduler::SnapshotCalls::PushSnapshot);

        // TODO - avoid copying data here
        flatbuffers::FlatBufferBuilder mb;
        auto keyOffset = mb.CreateString(key);
        auto dataOffset = mb.CreateVector<uint8_t>(data.data, data.size);
        auto requestOffset =
          CreateSnapshotPushRequest(mb, keyOffset, dataOffset);

        mb.Finish(requestOffset);
        uint8_t* msg = mb.GetBufferPointer();
        int size = mb.GetSize();
        send(msg, size);
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
        const auto& logger = faabric::util::getLogger();
        logger->debug("Pushing {} diffs for snapshot {} to {}",
                      diffs.size(),
                      snapshotKey,
                      host);

        // Send the header first
        sendHeader(faabric::scheduler::SnapshotCalls::PushSnapshotDiffs);

        // TODO - avoid copying data here
        flatbuffers::FlatBufferBuilder mb;

        // Create objects for all the chunks
        std::vector<flatbuffers::Offset<SnapshotDiffChunk>> diffsFbVector;
        for (const auto& d : diffs) {
            auto dataOffset = mb.CreateVector<uint8_t>(d.data, d.size);
            auto chunk = CreateSnapshotDiffChunk(mb, d.offset, dataOffset);
            diffsFbVector.push_back(chunk);
        }

        // Set up the main request
        auto keyOffset = mb.CreateString(snapshotKey);
        auto diffsOffset = mb.CreateVector(diffsFbVector);
        auto requestOffset =
          CreateSnapshotDiffPushRequest(mb, keyOffset, diffsOffset);

        // Send it
        mb.Finish(requestOffset);
        uint8_t* msg = mb.GetBufferPointer();
        int size = mb.GetSize();
        send(msg, size);
    }
}

void SnapshotClient::deleteSnapshot(const std::string& key)
{
    if (faabric::util::isMockMode()) {
        faabric::util::UniqueLock lock(mockMutex);
        snapshotDeletes.emplace_back(host, key);

    } else {
        const auto& logger = faabric::util::getLogger();
        logger->debug("Deleting snapshot {} from {}", key, host);

        // Send the header first
        sendHeader(faabric::scheduler::SnapshotCalls::DeleteSnapshot);

        // TODO - avoid copying data here
        flatbuffers::FlatBufferBuilder mb;
        auto keyOffset = mb.CreateString(key);
        auto requestOffset = CreateSnapshotDeleteRequest(mb, keyOffset);

        mb.Finish(requestOffset);
        uint8_t* msg = mb.GetBufferPointer();
        int size = mb.GetSize();
        send(msg, size);
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
        const auto& logger = faabric::util::getLogger();
        logger->debug(
          "Sending thread result for {} to {} (plus {} snapshot diffs)",
          messageId,
          host,
          diffs.size());

        flatbuffers::FlatBufferBuilder mb;

        // Create objects for all the chunks
        std::vector<flatbuffers::Offset<SnapshotDiffChunk>> diffsFbVector;
        for (const auto& d : diffs) {
            auto dataOffset = mb.CreateVector<uint8_t>(d.data, d.size);
            auto chunk = CreateSnapshotDiffChunk(mb, d.offset, dataOffset);
            diffsFbVector.push_back(chunk);
        }

        auto keyOffset = mb.CreateString(snapshotKey);
        auto diffsOffset = mb.CreateVector(diffsFbVector);
        auto requestOffset = CreateThreadResultRequest(
          mb, messageId, returnValue, keyOffset, diffsOffset);

        mb.Finish(requestOffset);

        // Send the header first
        sendHeader(faabric::scheduler::SnapshotCalls::ThreadResult);

        uint8_t* msg = mb.GetBufferPointer();
        int size = mb.GetSize();
        send(msg, size);
    }
}
}
