#include <faabric/redis/Redis.h>
#include <faabric/state/InMemoryStateRegistry.h>
#include <faabric/state/StateKeyValue.h>
#include <faabric/util/bytes.h>
#include <faabric/util/locks.h>
#include <faabric/util/logging.h>
#include <faabric/util/state.h>

#include <vector>

#define MAIN_KEY_PREFIX "main_"

namespace faabric::state {
InMemoryStateRegistry& getInMemoryStateRegistry()
{
    static InMemoryStateRegistry reg;
    return reg;
}

static std::string getMasterKey(const std::string& user, const std::string& key)
{
    std::string mainKey = MAIN_KEY_PREFIX + user + "_" + key;
    return mainKey;
}

std::string InMemoryStateRegistry::getMasterIP(const std::string& user,
                                               const std::string& key,
                                               const std::string& thisIP,
                                               bool claim)
{
    std::string lookupKey = faabric::util::keyForUser(user, key);

    // See if we already have the main
    {
        faabric::util::SharedLock lock(mainMapMutex);
        if (mainMap.count(lookupKey) > 0) {
            return mainMap[lookupKey];
        }
    }

    // No main found, need to establish

    // Acquire lock
    faabric::util::FullLock lock(mainMapMutex);

    // Double check condition
    if (mainMap.count(lookupKey) > 0) {
        return mainMap[lookupKey];
    }

    SPDLOG_TRACE("Checking main for state {}", lookupKey);

    // Query Redis
    const std::string mainKey = getMasterKey(user, key);
    redis::Redis& redis = redis::Redis::getState();
    std::vector<uint8_t> mainIPBytes = redis.get(mainKey);

    if (mainIPBytes.empty() && !claim) {
        // No main found and not claiming
        SPDLOG_TRACE("No main found for {}", lookupKey);
        throw StateKeyValueException("Found no main for state " + mainKey);
    }

    // If no main and we want to claim, attempt to do so
    if (mainIPBytes.empty()) {
        uint32_t mainLockId = StateKeyValue::waitOnRedisRemoteLock(mainKey);
        if (mainLockId == 0) {
            SPDLOG_ERROR("Unable to acquire remote lock for {}", mainKey);
            throw std::runtime_error("Unable to get remote lock");
        }

        SPDLOG_DEBUG("Claiming main for {} (this host {})", lookupKey, thisIP);

        // Check there's still no main, if so, claim
        mainIPBytes = redis.get(mainKey);
        if (mainIPBytes.empty()) {
            mainIPBytes = faabric::util::stringToBytes(thisIP);
            redis.set(mainKey, mainIPBytes);
        }

        redis.releaseLock(mainKey, mainLockId);
    }

    // Cache the result locally
    std::string mainIP = faabric::util::bytesToString(mainIPBytes);
    SPDLOG_DEBUG(
      "Caching main for {} as {} (this host {})", lookupKey, mainIP, thisIP);

    mainMap[lookupKey] = mainIP;

    return mainIP;
}

std::string InMemoryStateRegistry::getMasterIPForOtherMaster(
  const std::string& userIn,
  const std::string& keyIn,
  const std::string& thisIP)
{
    // Get the main IP
    std::string mainIP = getMasterIP(userIn, keyIn, thisIP, false);

    // Sanity check that the main is *not* this machine
    if (mainIP == thisIP) {
        SPDLOG_ERROR("Attempting to pull state size on main ({}/{} on {})",
                     userIn,
                     keyIn,
                     thisIP);
        throw std::runtime_error("Attempting to pull state size on main");
    }

    return mainIP;
}

void InMemoryStateRegistry::clear()
{
    faabric::util::FullLock lock(mainMapMutex);
    mainMap.clear();
}

}
