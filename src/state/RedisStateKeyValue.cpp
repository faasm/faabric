#include <faabric/state/RedisStateKeyValue.h>
#include <faabric/util/logging.h>
#include <faabric/util/macros.h>
#include <faabric/util/state.h>
#include <faabric/util/timing.h>

/**
 * WARNING - key-value objects are shared between threads, BUT
 * hiredis is not thread-safe, so make sure you always retrieve
 * the reference to Redis inline rather than sharing a reference
 * within the class.
 */

namespace faabric::state {
RedisStateKeyValue::RedisStateKeyValue(const std::string& userIn,
                                       const std::string& keyIn,
                                       size_t sizeIn)
  : StateKeyValue(userIn, keyIn, sizeIn)
  , joinedKey(faabric::util::keyForUser(user, key)){

  };

RedisStateKeyValue::RedisStateKeyValue(const std::string& userIn,
                                       const std::string& keyIn)
  : RedisStateKeyValue(userIn, keyIn, 0){

  };

size_t RedisStateKeyValue::getStateSizeFromRemote(const std::string& userIn,
                                                  const std::string& keyIn)
{
    std::string actualKey = faabric::util::keyForUser(userIn, keyIn);
    return redis::Redis::getState().strlen(actualKey);
}

void RedisStateKeyValue::deleteFromRemote(const std::string& userIn,
                                          const std::string& keyIn)
{
    std::string actualKey = faabric::util::keyForUser(userIn, keyIn);
    redis::Redis::getState().del(actualKey);
}

void RedisStateKeyValue::clearAll(bool global)
{
    if (global) {
        redis::Redis::getState().flushAll();
    }
}

void RedisStateKeyValue::pullFromRemote()
{
    PROF_START(statePull)

    // Read from the remote
    SPDLOG_DEBUG("Pulling remote value for {}", joinedKey);
    auto memoryBytes = static_cast<uint8_t*>(sharedMemory);
    redis::Redis::getState().get(joinedKey, memoryBytes, valueSize);

    PROF_END(statePull)
}

void RedisStateKeyValue::pullChunkFromRemote(long offset, size_t length)
{
    PROF_START(stateChunkPull)

    SPDLOG_DEBUG("Pulling remote chunk ({}-{}) for {}",
                 offset,
                 offset + length,
                 joinedKey);

    // Redis ranges are inclusive, so we need to knock one off
    size_t rangeStart = offset;
    size_t rangeEnd = offset + length - 1;

    // Read from the remote
    auto buffer = BYTES(sharedMemory) + offset;
    redis::Redis::getState().getRange(
      joinedKey, buffer, length, rangeStart, rangeEnd);

    PROF_END(stateChunkPull)
}

void RedisStateKeyValue::pushToRemote()
{
    PROF_START(pushFull)

    SPDLOG_DEBUG("Pushing whole value for {}", joinedKey);

    redis::Redis::getState().set(
      joinedKey, static_cast<uint8_t*>(sharedMemory), valueSize);

    PROF_END(pushFull)
}

void RedisStateKeyValue::pushPartialToRemote(
  const std::vector<StateChunk>& chunks)
{
    PROF_START(pushPartial)

    redis::Redis& redis = redis::Redis::getState();

    // Pipeline the updates
    for (auto& c : chunks) {
        redis.setRangePipeline(joinedKey, c.offset, c.data, c.length);
    }

    // Flush the pipeline
    SPDLOG_DEBUG("Pipelined {} updates on {}", chunks.size(), joinedKey);
    redis.flushPipeline(chunks.size());

    PROF_END(pushPartial)
}

void RedisStateKeyValue::appendToRemote(const uint8_t* data, size_t length)
{
    redis::Redis::getState().enqueueBytes(joinedKey, data, length);
}

void RedisStateKeyValue::pullAppendedFromRemote(uint8_t* data,
                                                size_t length,
                                                long nValues)
{
    redis::Redis::getState().dequeueMultiple(joinedKey, data, length, nValues);
}

void RedisStateKeyValue::clearAppendedFromRemote()
{
    redis::Redis::getState().del(joinedKey);
}
}
