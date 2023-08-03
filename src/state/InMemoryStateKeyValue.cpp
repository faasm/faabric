#include <faabric/state/InMemoryStateKeyValue.h>

#include <cstdio>

#include <faabric/util/bytes.h>
#include <faabric/util/logging.h>
#include <faabric/util/macros.h>
#include <faabric/util/state.h>

namespace faabric::state {
// --------------------------------------------
// Static properties and methods
// --------------------------------------------

size_t InMemoryStateKeyValue::getStateSizeFromRemote(const std::string& userIn,
                                                     const std::string& keyIn,
                                                     const std::string& thisIP)
{
    std::string mainIP;
    try {
        mainIP = getInMemoryStateRegistry().getMasterIPForOtherMaster(
          userIn, keyIn, thisIP);
    } catch (StateKeyValueException& ex) {
        return 0;
    }

    StateClient stateClient(userIn, keyIn, mainIP);
    size_t stateSize = stateClient.stateSize();
    return stateSize;
}

void InMemoryStateKeyValue::deleteFromRemote(const std::string& userIn,
                                             const std::string& keyIn,
                                             const std::string& thisIPIn)
{
    InMemoryStateRegistry& reg = getInMemoryStateRegistry();
    std::string mainIP = reg.getMasterIP(userIn, keyIn, thisIPIn, false);

    // Ignore if we're the main
    if (mainIP == thisIPIn) {
        return;
    }

    StateClient stateClient(userIn, keyIn, mainIP);
    stateClient.deleteState();
}

void InMemoryStateKeyValue::clearAll(bool global)
{
    InMemoryStateRegistry& reg = state::getInMemoryStateRegistry();
    reg.clear();
}

// --------------------------------------------
// Class definition
// --------------------------------------------

InMemoryStateKeyValue::InMemoryStateKeyValue(const std::string& userIn,
                                             const std::string& keyIn,
                                             size_t sizeIn,
                                             const std::string& thisIPIn)
  : StateKeyValue(userIn, keyIn, sizeIn)
  , thisIP(thisIPIn)
  , mainIP(getInMemoryStateRegistry().getMasterIP(user, key, thisIP, true))
  , status(mainIP == thisIP ? InMemoryStateKeyStatus::MAIN
                            : InMemoryStateKeyStatus::NOT_MAIN)
  , stateRegistry(getInMemoryStateRegistry())
{
    SPDLOG_TRACE("Creating in-memory state key-value for {}/{} size {} (this "
                 "host {}, main {})",
                 userIn,
                 keyIn,
                 sizeIn,
                 thisIP,
                 mainIP);
}

InMemoryStateKeyValue::InMemoryStateKeyValue(const std::string& userIn,
                                             const std::string& keyIn,
                                             const std::string& thisIPIn)
  : InMemoryStateKeyValue(userIn, keyIn, 0, thisIPIn)
{}

bool InMemoryStateKeyValue::isMaster()
{
    return status == InMemoryStateKeyStatus::MAIN;
}

// ----------------------------------------
// Normal state key-value API
// ----------------------------------------

void InMemoryStateKeyValue::pullFromRemote()
{
    if (status == InMemoryStateKeyStatus::MAIN) {
        return;
    }

    std::vector<StateChunk> chunks = getAllChunks();
    StateClient cli(user, key, mainIP);
    cli.pullChunks(chunks, BYTES(sharedMemory));
}

void InMemoryStateKeyValue::pullChunkFromRemote(long offset, size_t length)
{
    if (status == InMemoryStateKeyStatus::MAIN) {
        return;
    }

    uint8_t* chunkStart = BYTES(sharedMemory) + offset;
    std::vector<StateChunk> chunks = { StateChunk(offset, length, chunkStart) };
    StateClient cli(user, key, mainIP);
    cli.pullChunks(chunks, BYTES(sharedMemory));
}

void InMemoryStateKeyValue::pushToRemote()
{
    if (status == InMemoryStateKeyStatus::MAIN) {
        return;
    }

    std::vector<StateChunk> allChunks = getAllChunks();
    StateClient cli(user, key, mainIP);
    cli.pushChunks(allChunks);
}

void InMemoryStateKeyValue::pushPartialToRemote(
  const std::vector<StateChunk>& chunks)
{
    if (status == InMemoryStateKeyStatus::MAIN) {
        // Nothing to be done
    } else {
        StateClient cli(user, key, mainIP);
        cli.pushChunks(chunks);
    }
}

void InMemoryStateKeyValue::appendToRemote(const uint8_t* data, size_t length)
{
    if (status == InMemoryStateKeyStatus::MAIN) {
        // Create new memory region to hold data
        auto dataCopy = std::make_unique<uint8_t[]>(length);
        std::copy(data, data + length, dataCopy.get());

        // Add to list
        appendedData.emplace_back(length, std::move(dataCopy));
    } else {
        StateClient cli(user, key, mainIP);
        cli.append(data, length);
    }
}

void InMemoryStateKeyValue::pullAppendedFromRemote(uint8_t* data,
                                                   size_t length,
                                                   long nValues)
{
    if (status == InMemoryStateKeyStatus::MAIN) {
        // Copy all appended data into buffer locally
        size_t offset = 0;
        for (int i = 0; i < nValues; i++) {
            AppendedInMemoryState& appended = appendedData.at(i);
            uint8_t* dataStart = appended.data.get();
            std::copy(dataStart, dataStart + appended.length, data + offset);
            offset += appended.length;
        }
    } else {
        StateClient cli(user, key, mainIP);
        cli.pullAppended(data, length, nValues);
    }
}

void InMemoryStateKeyValue::clearAppendedFromRemote()
{
    if (status == InMemoryStateKeyStatus::MAIN) {
        // Clear appended locally
        appendedData.clear();
    } else {
        StateClient cli(user, key, mainIP);
        cli.clearAppended();
    }
}

AppendedInMemoryState& InMemoryStateKeyValue::getAppendedValue(uint idx)
{
    return appendedData.at(idx);
}
}
