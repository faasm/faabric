#include <faabric/snapshot/SnapshotRegistry.h>
#include <faabric/util/func.h>
#include <faabric/util/locks.h>
#include <faabric/util/logging.h>
#include <faabric/util/memory.h>

#include <sys/mman.h>

namespace faabric::snapshot {
SnapshotRegistry::SnapshotRegistry() {}

faabric::util::SnapshotData& SnapshotRegistry::getSnapshot(
  const std::string& key)
{
    if (key.empty()) {
        SPDLOG_ERROR("Attempting to get snapshot with empty key");
        throw std::runtime_error("Getting snapshot with empty key");
    }

    if (snapshotMap.count(key) == 0) {
        SPDLOG_ERROR("Snapshot for {} does not exist", key);
        throw std::runtime_error("Snapshot doesn't exist");
    }

    return snapshotMap[key];
}

bool SnapshotRegistry::snapshotExists(const std::string& key)
{
    return snapshotMap.find(key) != snapshotMap.end();
}

void SnapshotRegistry::mapSnapshot(const std::string& key, uint8_t* target)
{
    faabric::util::SnapshotData& d = getSnapshot(key);
    d.mapToMemory(target);
}

void SnapshotRegistry::takeSnapshotIfNotExists(const std::string& key,
                                               faabric::util::SnapshotData &data,
                                               bool locallyRestorable)
{
    doTakeSnapshot(key, data, locallyRestorable, false);
}

void SnapshotRegistry::takeSnapshot(const std::string& key,
                                    faabric::util::SnapshotData &data,
                                    bool locallyRestorable)
{
    doTakeSnapshot(key, data, locallyRestorable, true);
}

void SnapshotRegistry::doTakeSnapshot(const std::string& key,
                                      const faabric::util::SnapshotData& data,
                                      bool locallyRestorable,
                                      bool overwrite)
{
    if (data.size == 0) {
        SPDLOG_ERROR("Cannot take snapshot {} of size zero", key);
        throw std::runtime_error("Taking snapshot size zero");
    }

    faabric::util::UniqueLock lock(snapshotsMx);

    if (snapshotExists(key) && !overwrite) {
        SPDLOG_TRACE("Skipping already existing snapshot {}", key);
        return;
    }

    SPDLOG_TRACE("Registering snapshot {} size {} (restorable={})",
                 key,
                 data.size,
                 locallyRestorable);

    // Note - we only preserve the snapshot in the in-memory file, and do not
    // take ownership for the original data referenced in SnapshotData
    faabric::util::SnapshotData& recordedData = snapshotMap[key];
    recordedData.size = data.size;
    recordedData.data = data.data;

    // Write to fd to be locally restorable
    if (locallyRestorable) {
        recordedData.writeToFd(key);
    }
}

void SnapshotRegistry::deleteSnapshot(const std::string& key)
{
    faabric::util::UniqueLock lock(snapshotsMx);

    if (snapshotMap.count(key) == 0) {
        return;
    }

    snapshotMap.erase(key);
}

void SnapshotRegistry::changeSnapshotSize(const std::string& key,
                                            size_t newSize)
{
    faabric::util::UniqueLock lock(snapshotsMx);

    faabric::util::SnapshotData& d = getSnapshot(key);
    d.setSnapshotSize(newSize);
}

size_t SnapshotRegistry::getSnapshotCount()
{
    faabric::util::UniqueLock lock(snapshotsMx);
    return snapshotMap.size();
}

SnapshotRegistry& getSnapshotRegistry()
{
    static SnapshotRegistry reg;
    return reg;
}

void SnapshotRegistry::clear()
{
    snapshotMap.clear();
}
}
