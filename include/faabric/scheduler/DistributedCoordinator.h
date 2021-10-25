#pragma once

#include <faabric/proto/faabric.pb.h>
#include <faabric/scheduler/DistributedCoordinationGroup.h>

namespace faabric::scheduler {

class DistributedCoordinator
{
  public:
    DistributedCoordinator();

    void clear();

    bool groupExists(int32_t groupId);

    std::shared_ptr<DistributedCoordinationGroup> initGroup(
      const std::string& masterHost,
      int32_t groupId,
      int32_t groupSize);

    std::shared_ptr<DistributedCoordinationGroup> initGroup(
      const faabric::Message& msg);

    std::shared_ptr<DistributedCoordinationGroup> getCoordinationGroup(
      int32_t groupId);

  private:
    std::shared_mutex sharedMutex;

    std::unordered_map<int32_t, std::shared_ptr<DistributedCoordinationGroup>>
      groups;

    faabric::util::SystemConfig& conf;
};

DistributedCoordinator& getDistributedCoordinator();
}
