#pragma once

#include <shared_mutex>
#include <string>
#include <unordered_map>

#include <faabric/proto/faabric.pb.h>
#include <faabric/util/locks.h>
#include <faabric/util/snapshot.h>

namespace faabric::snapshot {

class SnapshotRegistry
{
  public:
    SnapshotRegistry() = default;

    std::shared_ptr<faabric::util::SnapshotData> getSnapshot(
      const std::string& key);

    bool snapshotExists(const std::string& key);

    void registerSnapshot(const std::string& key,
                          std::shared_ptr<faabric::util::SnapshotData> data);

    void deleteSnapshot(const std::string& key);

    size_t getSnapshotCount();

    void clear();

  private:
    std::unordered_map<std::string,
                       std::shared_ptr<faabric::util::SnapshotData>>
      snapshotMap;

    std::shared_mutex snapshotsMx;

    int writeSnapshotToFd(const std::string& key,
                          faabric::util::SnapshotData& data);
};

SnapshotRegistry& getSnapshotRegistry();

}
