#include <faabric/state/InMemoryStateKeyValue.h>
#include <faabric/transport/MessageContext.h>

#include <cstdio>

#include <faabric/util/bytes.h>
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
    std::string masterIP;
    try {
        masterIP = getInMemoryStateRegistry().getMasterIPForOtherMaster(
          userIn, keyIn, thisIP);
    } catch (StateKeyValueException& ex) {
        return 0;
    }

    StateClient stateClient(userIn, keyIn, masterIP);
    size_t stateSize = stateClient.stateSize();
    stateClient.close();
    return stateSize;
}

void InMemoryStateKeyValue::deleteFromRemote(const std::string& userIn,
                                             const std::string& keyIn,
                                             const std::string& thisIPIn)
{
    InMemoryStateRegistry& reg = getInMemoryStateRegistry();
    std::string masterIP = reg.getMasterIP(userIn, keyIn, thisIPIn, false);

    // Ignore if we're the master
    if (masterIP == thisIPIn) {
        return;
    }

    StateClient stateClient(userIn, keyIn, masterIP);
    stateClient.deleteState();
    stateClient.close();
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
  , masterIP(getInMemoryStateRegistry().getMasterIP(user, key, thisIP, true))
  , status(masterIP == thisIP ? InMemoryStateKeyStatus::MASTER
                              : InMemoryStateKeyStatus::NOT_MASTER)
  , stateRegistry(getInMemoryStateRegistry())
{}

InMemoryStateKeyValue::InMemoryStateKeyValue(const std::string& userIn,
                                             const std::string& keyIn,
                                             const std::string& thisIPIn)
  : InMemoryStateKeyValue(userIn, keyIn, 0, thisIPIn)
{}

bool InMemoryStateKeyValue::isMaster()
{
    return status == InMemoryStateKeyStatus::MASTER;
}

// ----------------------------------------
// Normal state key-value API
// ----------------------------------------

void InMemoryStateKeyValue::lockGlobal()
{
    if (status == InMemoryStateKeyStatus::MASTER) {
        globalLock.lock();
    } else {
        StateClient cli(user, key, masterIP);
        cli.lock();
        cli.close();
    }
}

void InMemoryStateKeyValue::unlockGlobal()
{
    if (status == InMemoryStateKeyStatus::MASTER) {
        globalLock.unlock();
    } else {
        StateClient cli(user, key, masterIP);
        cli.unlock();
        cli.close();
    }
}

void InMemoryStateKeyValue::pullFromRemote()
{
    if (status == InMemoryStateKeyStatus::MASTER) {
        return;
    }

    std::vector<StateChunk> chunks = getAllChunks();
    StateClient cli(user, key, masterIP);
    cli.pullChunks(chunks, BYTES(sharedMemory));
    cli.close();
}

void InMemoryStateKeyValue::pullChunkFromRemote(long offset, size_t length)
{
    if (status == InMemoryStateKeyStatus::MASTER) {
        return;
    }

    uint8_t* chunkStart = BYTES(sharedMemory) + offset;
    std::vector<StateChunk> chunks = { StateChunk(offset, length, chunkStart) };
    StateClient cli(user, key, masterIP);
    cli.pullChunks(chunks, BYTES(sharedMemory));
    cli.close();
}

void InMemoryStateKeyValue::pushToRemote()
{
    if (status == InMemoryStateKeyStatus::MASTER) {
        return;
    }

    std::vector<StateChunk> allChunks = getAllChunks();
    StateClient cli(user, key, masterIP);
    cli.pushChunks(allChunks);
    cli.close();
}

void InMemoryStateKeyValue::pushPartialToRemote(
  const std::vector<StateChunk>& chunks)
{
    if (status == InMemoryStateKeyStatus::MASTER) {
        // Nothing to be done
    } else {
        StateClient cli(user, key, masterIP);
        cli.pushChunks(chunks);
        cli.close();
    }
}

void InMemoryStateKeyValue::appendToRemote(const uint8_t* data, size_t length)
{
    if (status == InMemoryStateKeyStatus::MASTER) {
        // Create new memory region to hold data
        auto dataCopy = new uint8_t[length];
        std::copy(data, data + length, dataCopy);

        // Add to list
        appendedData.emplace_back(length, dataCopy);
    } else {
        StateClient cli(user, key, masterIP);
        cli.append(data, length);
        cli.close();
    }
}

void InMemoryStateKeyValue::pullAppendedFromRemote(uint8_t* data,
                                                   size_t length,
                                                   long nValues)
{
    if (status == InMemoryStateKeyStatus::MASTER) {
        // Copy all appended data into buffer locally
        size_t offset = 0;
        for (int i = 0; i < nValues; i++) {
            AppendedInMemoryState& appended = appendedData.at(i);
            uint8_t* dataStart = appended.data.get();
            std::copy(dataStart, dataStart + appended.length, data + offset);
            offset += appended.length;
        }
    } else {
        StateClient cli(user, key, masterIP);
        cli.pullAppended(data, length, nValues);
        cli.close();
    }
}

void InMemoryStateKeyValue::clearAppendedFromRemote()
{
    if (status == InMemoryStateKeyStatus::MASTER) {
        // Clear appended locally
        appendedData.clear();
    } else {
        StateClient cli(user, key, masterIP);
        cli.clearAppended();
        cli.close();
    }
}

AppendedInMemoryState& InMemoryStateKeyValue::getAppendedValue(uint idx)
{
    return appendedData.at(idx);
}
}
