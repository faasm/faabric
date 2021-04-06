#include <faabric/snapshot/SnapshotRegistry.h>
#include <faabric/util/func.h>
#include <faabric/util/locks.h>
#include <faabric/util/logging.h>

#include <sys/mman.h>

namespace faabric::snapshot {
SnapshotRegistry::SnapshotRegistry() {}

faabric::util::SnapshotData& SnapshotRegistry::getSnapshot(
  const std::string& key)
{
    auto logger = faabric::util::getLogger();
    if (snapshotMap.count(key) == 0) {
        logger->error("Snapshot for {} does not exist", key);
        throw std::runtime_error("Snapshot doesn't exist");
    }

    return snapshotMap[key];
}

void SnapshotRegistry::mapSnapshot(const std::string& key, uint8_t* target)
{
    faabric::util::SnapshotData d = getSnapshot(key);
    mmap(target, d.size, PROT_WRITE, MAP_PRIVATE | MAP_FIXED, d.fd, 0);
}

void SnapshotRegistry::takeSnapshot(const std::string& key,
                                    faabric::util::SnapshotData data)
{
    // Note - we only preserve the snapshot in the in-memory file, and do not
    // take ownership for the original data referenced in SnapshotData
    faabric::util::UniqueLock lock(snapshotsMx);
    snapshotMap[key] = data;

    writeSnapshotToFd(key);
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
    snapshotMap.clear();
}

int SnapshotRegistry::writeSnapshotToFd(const std::string& key)
{
    auto logger = faabric::util::getLogger();

    int fd = ::memfd_create(key.c_str(), 0);
    faabric::util::SnapshotData snapData = getSnapshot(key);

    // Make the fd big enough
    int ferror = ::ftruncate(fd, snapData.size);
    if (ferror) {
        logger->error("ferror call failed with error {}", ferror);
        throw std::runtime_error("Failed writing memory to fd (ftruncate)");
    }

    // Write the data
    ssize_t werror = ::write(fd, snapData.data, snapData.size);
    if (werror == -1) {
        logger->error("Write call failed with error {}", werror);
        throw std::runtime_error("Failed writing memory to fd (write)");
    }

    // Record the fd
    getSnapshot(key).fd = fd;

    logger->debug("Wrote snapshot {} to fd {}", key, fd);
    return fd;
}
}
