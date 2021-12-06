#include <faabric/snapshot/SnapshotRegistry.h>
#include <faabric/util/func.h>
#include <faabric/util/locks.h>
#include <faabric/util/logging.h>
#include <faabric/util/memory.h>

#include <sys/mman.h>

namespace faabric::snapshot {
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
    faabric::util::SnapshotData d = getSnapshot(key);

    if (!faabric::util::isPageAligned((void*)target)) {
        SPDLOG_ERROR(
          "Mapping snapshot {} to non page-aligned address {}", key, target);
        throw std::runtime_error(
          "Mapping snapshot to non page-aligned address");
    }

    if (d.fd == 0) {
        SPDLOG_ERROR("Attempting to map non-restorable snapshot");
        throw std::runtime_error("Mapping non-restorable snapshot");
    }

    void* mmapRes =
      mmap(target, d.size, PROT_WRITE, MAP_PRIVATE | MAP_FIXED, d.fd, 0);

    if (mmapRes == MAP_FAILED) {
        SPDLOG_ERROR(
          "mmapping snapshot failed: {} ({})", errno, ::strerror(errno));
        throw std::runtime_error("mmapping snapshot failed");
    }
}

void SnapshotRegistry::takeSnapshotIfNotExists(const std::string& key,
                                               faabric::util::SnapshotData data,
                                               bool locallyRestorable)
{
    doTakeSnapshot(key, data, locallyRestorable, false);
}

void SnapshotRegistry::takeSnapshot(const std::string& key,
                                    faabric::util::SnapshotData data,
                                    bool locallyRestorable)
{
    doTakeSnapshot(key, data, locallyRestorable, true);
}

void SnapshotRegistry::doTakeSnapshot(const std::string& key,
                                      faabric::util::SnapshotData data,
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
    snapshotMap[key] = data;

    // Write to fd to be locally restorable
    if (locallyRestorable) {
        writeSnapshotToFd(key);
    }
}

void SnapshotRegistry::deleteSnapshot(const std::string& key)
{
    faabric::util::UniqueLock lock(snapshotsMx);

    if (snapshotMap.count(key) == 0) {
        return;
    }

    faabric::util::SnapshotData d = snapshotMap[key];

    // Note - the data referenced by the SnapshotData object is not owned by the
    // snapshot registry so we don't delete it here. We only remove the file
    // descriptor used for mapping memory
    if (d.fd > 0) {
        ::close(d.fd);
    }

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
        if (p.second.fd > 0) {
            ::close(p.second.fd);
        }
    }

    snapshotMap.clear();
}

int SnapshotRegistry::writeSnapshotToFd(const std::string& key)
{
    int fd = ::memfd_create(key.c_str(), 0);
    faabric::util::SnapshotData snapData = getSnapshot(key);

    // Make the fd big enough
    int ferror = ::ftruncate(fd, snapData.size);
    if (ferror) {
        SPDLOG_ERROR("ferror call failed with error {}", ferror);
        throw std::runtime_error("Failed writing memory to fd (ftruncate)");
    }

    // Write the data
    ssize_t werror = ::write(fd, snapData.data, snapData.size);
    if (werror == -1) {
        SPDLOG_ERROR("Write call failed with error {}", werror);
        throw std::runtime_error("Failed writing memory to fd (write)");
    }

    // Record the fd
    getSnapshot(key).fd = fd;

    SPDLOG_DEBUG("Wrote snapshot {} to fd {}", key, fd);
    return fd;
}
}
