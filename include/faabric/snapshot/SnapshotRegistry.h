#pragma once

#include <mutex>
#include <string>
#include <unordered_map>

#include <faabric/util/snapshot.h>

namespace faabric::snapshot {

class SnapshotRegistry
{
  public:
    SnapshotRegistry();

    faabric::util::SnapshotData getSnapshot(const std::string& key);

    void setSnapshot(const std::string& key, faabric::util::SnapshotData data);

    void deleteSnapshot(const std::string& key);

  private:
    std::unordered_map<std::string, faabric::util::SnapshotData> snapshotMap;

    std::mutex snapshotsMx;
};

SnapshotRegistry& getSnapshotRegistry();

}
