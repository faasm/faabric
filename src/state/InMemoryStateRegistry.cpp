#include <faabric/redis/Redis.h>
#include <faabric/state/InMemoryStateRegistry.h>
#include <faabric/state/StateKeyValue.h>
#include <faabric/util/bytes.h>
#include <faabric/util/locks.h>
#include <faabric/util/logging.h>
#include <faabric/util/state.h>

#include <vector>

#define MASTER_KEY_PREFIX "master_"

namespace faabric::state {
InMemoryStateRegistry& getInMemoryStateRegistry()
{
    static InMemoryStateRegistry reg;
    return reg;
}

static std::string getMasterKey(const std::string& user, const std::string& key)
{
    std::string masterKey = MASTER_KEY_PREFIX + user + "_" + key;
    return masterKey;
}

std::string InMemoryStateRegistry::getMasterIP(const std::string& user,
                                               const std::string& key,
                                               const std::string& thisIP,
                                               bool claim)
{
    std::string lookupKey = faabric::util::keyForUser(user, key);

    // See if we already have the master
    {
        faabric::util::SharedLock lock(masterMapMutex);
        if (masterMap.count(lookupKey) > 0) {
            return masterMap[lookupKey];
        }
    }

    // No master found, need to establish

    // Acquire lock
    faabric::util::FullLock lock(masterMapMutex);

    // Double check condition
    if (masterMap.count(lookupKey) > 0) {
        return masterMap[lookupKey];
    }

    SPDLOG_TRACE("Checking master for state {}", lookupKey);

    // Query Redis
    const std::string masterKey = getMasterKey(user, key);
    redis::Redis& redis = redis::Redis::getState();
    std::vector<uint8_t> masterIPBytes = redis.get(masterKey);

    if (masterIPBytes.empty() && !claim) {
        // No master found and not claiming
        SPDLOG_TRACE("No master found for {}", lookupKey);
        throw StateKeyValueException("Found no master for state " + masterKey);
    }

    // If no master and we want to claim, attempt to do so
    if (masterIPBytes.empty()) {
        uint32_t masterLockId = StateKeyValue::waitOnRedisRemoteLock(masterKey);
        if (masterLockId == 0) {
            SPDLOG_ERROR("Unable to acquire remote lock for {}", masterKey);
            throw std::runtime_error("Unable to get remote lock");
        }

        SPDLOG_DEBUG(
          "Claiming master for {} (this host {})", lookupKey, thisIP);

        // Check there's still no master, if so, claim
        masterIPBytes = redis.get(masterKey);
        if (masterIPBytes.empty()) {
            masterIPBytes = faabric::util::stringToBytes(thisIP);
            redis.set(masterKey, masterIPBytes);
        }

        redis.releaseLock(masterKey, masterLockId);
    }

    // Cache the result locally
    std::string masterIP = faabric::util::bytesToString(masterIPBytes);
    SPDLOG_DEBUG("Caching master for {} as {} (this host {})",
                 lookupKey,
                 masterIP,
                 thisIP);

    masterMap[lookupKey] = masterIP;

    return masterIP;
}

std::string InMemoryStateRegistry::getMasterIPForOtherMaster(
  const std::string& userIn,
  const std::string& keyIn,
  const std::string& thisIP)
{
    // Get the master IP
    std::string masterIP = getMasterIP(userIn, keyIn, thisIP, false);

    // Sanity check that the master is *not* this machine
    if (masterIP == thisIP) {
        SPDLOG_ERROR("Attempting to pull state size on master ({}/{} on {})",
                     userIn,
                     keyIn,
                     thisIP);
        throw std::runtime_error("Attempting to pull state size on master");
    }

    return masterIP;
}

void InMemoryStateRegistry::clear()
{
    faabric::util::FullLock lock(masterMapMutex);
    masterMap.clear();
}

}
