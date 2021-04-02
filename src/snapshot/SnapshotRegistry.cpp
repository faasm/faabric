#include <faabric/snapshot/SnapshotRegistry.h>
#include <faabric/util/locks.h>
#include <faabric/util/logging.h>

namespace faabric::snapshot {
SnapshotRegistry::SnapshotRegistry() {}

faabric::util::SnapshotData SnapshotRegistry::getSnapshot(
  const std::string& key)
{
    auto logger = faabric::util::getLogger();
    if (snapshotMap.count(key) == 0) {
        logger->error("Snapshot for {} does not exist", key);
        throw std::runtime_error("Snapshot doesn't exist");
    }

    return snapshotMap[key];
}

void SnapshotRegistry::setSnapshot(const std::string& key,
                                   faabric::util::SnapshotData data)
{
    faabric::util::UniqueLock lock(snapshotsMx);
    snapshotMap[key] = data;
}

void SnapshotRegistry::deleteSnapshot(const std::string& key)
{
    faabric::util::UniqueLock lock(snapshotsMx);

    if (snapshotMap.count(key) == 0) {
        return;
    }

    faabric::util::SnapshotData d = snapshotMap[key];
    delete[] d.data;
    snapshotMap.erase(key);
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
    for (auto p : snapshotMap) {
        deleteSnapshot(p.first);
    }

    snapshotMap.clear();
}
}
