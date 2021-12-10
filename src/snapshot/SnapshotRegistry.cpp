#include <faabric/snapshot/SnapshotRegistry.h>
#include <faabric/util/func.h>
#include <faabric/util/locks.h>
#include <faabric/util/logging.h>
#include <faabric/util/memory.h>
#include <faabric/util/snapshot.h>

#include <sys/mman.h>

namespace faabric::snapshot {
std::shared_ptr<faabric::util::SnapshotData> SnapshotRegistry::getSnapshot(
  const std::string& key)
{
    faabric::util::SharedLock lock(snapshotsMx);

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
    auto d = getSnapshot(key);
    d->mapToMemory(target);
}

void SnapshotRegistry::registerSnapshotIfNotExists(
  const std::string& key,
  std::shared_ptr<faabric::util::SnapshotData> data)
{
    doRegisterSnapshot(key, data, false);
}

void SnapshotRegistry::registerSnapshot(
  const std::string& key,
  std::shared_ptr<faabric::util::SnapshotData> data)
{
    doRegisterSnapshot(key, data, true);
}

void SnapshotRegistry::doRegisterSnapshot(
  const std::string& key,
  std::shared_ptr<faabric::util::SnapshotData> data,
  bool overwrite)
{
    if (data->size == 0) {
        SPDLOG_ERROR("Cannot take snapshot {} of size zero", key);
        throw std::runtime_error("Taking snapshot size zero");
    }

    faabric::util::FullLock lock(snapshotsMx);

    if (snapshotExists(key) && !overwrite) {
        SPDLOG_TRACE("Skipping already existing snapshot {}", key);
        return;
    }

    SPDLOG_TRACE("Registering snapshot {} size {}", key, data->size);

    snapshotMap[key] = data;
}

void SnapshotRegistry::deleteSnapshot(const std::string& key)
{
    faabric::util::FullLock lock(snapshotsMx);

    if (snapshotMap.count(key) == 0) {
        return;
    }

    snapshotMap.erase(key);
}

size_t SnapshotRegistry::getSnapshotCount()
{
    faabric::util::FullLock lock(snapshotsMx);
    return snapshotMap.size();
}

SnapshotRegistry& getSnapshotRegistry()
{
    static SnapshotRegistry reg;
    return reg;
}

void SnapshotRegistry::clear()
{
    faabric::util::FullLock lock(snapshotsMx);
    snapshotMap.clear();
}
}
