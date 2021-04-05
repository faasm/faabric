#pragma once

#include <mutex>
#include <string>
#include <unordered_map>

#include <faabric/proto/faabric.pb.h>
#include <faabric/util/snapshot.h>

namespace faabric::snapshot {

class SnapshotRegistry
{
  public:
    SnapshotRegistry();

    faabric::util::SnapshotData& getSnapshot(const std::string& key);

    void setBaseSnapshot(const faabric::Message& msg,
                         faabric::util::SnapshotData& data);

    void mapBaseSnapshot(const faabric::Message& msg, uint8_t* target);

    void mapSnapshot(const std::string& key, uint8_t* target);

    void setSnapshot(const std::string& key, faabric::util::SnapshotData data);

    void deleteSnapshot(const std::string& key);

    size_t getSnapshotCount();

    void clear();

  private:
    std::unordered_map<std::string, faabric::util::SnapshotData> snapshotMap;

    int writeSnapshotToFd(const std::string &key);

    std::mutex snapshotsMx;
};

SnapshotRegistry& getSnapshotRegistry();

}
