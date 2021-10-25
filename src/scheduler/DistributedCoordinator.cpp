#include <faabric/scheduler/DistributedCoordinator.h>
#include <faabric/util/config.h>
#include <faabric/util/locks.h>
#include <faabric/util/logging.h>

namespace faabric::scheduler {

DistributedCoordinator::DistributedCoordinator()
  : conf(faabric::util::getSystemConfig())
{}

std::shared_ptr<DistributedCoordinationGroup>
DistributedCoordinator::getCoordinationGroup(int32_t groupId)
{
    if (groups.find(groupId) == groups.end()) {
        SPDLOG_ERROR("Did not find group ID {} on this host", groupId);
        throw std::runtime_error("Group ID not found on host");
    }

    return groups.at(groupId);
}

std::shared_ptr<DistributedCoordinationGroup> DistributedCoordinator::initGroup(
  const std::string& masterHost,
  int32_t groupId,
  int32_t groupSize)
{
    if (groups.find(groupId) == groups.end()) {
        faabric::util::FullLock lock(sharedMutex);
        if (groups.find(groupId) == groups.end()) {
            groups.emplace(
              std::make_pair(groupId,
                             std::make_shared<DistributedCoordinationGroup>(
                               masterHost, groupId, groupSize)));
        }
    }

    {
        faabric::util::SharedLock lock(sharedMutex);
        return groups.at(groupId);
    }
}

std::shared_ptr<DistributedCoordinationGroup> DistributedCoordinator::initGroup(
  const faabric::Message& msg)
{
    return initGroup(msg.masterhost(), msg.groupid(), msg.groupsize());
}

DistributedCoordinator& getDistributedCoordinator()
{
    static DistributedCoordinator sync;
    return sync;
}

void DistributedCoordinator::clear()
{
    groups.clear();
}

bool DistributedCoordinator::groupExists(int32_t groupId)
{
    faabric::util::SharedLock lock(sharedMutex);
    return groups.find(groupId) != groups.end();
}
}
