#pragma once

#include <faabric/batch-scheduler/SchedulingDecision.h>
#include <faabric/util/batch.h>

#include <string>

#define DO_NOT_MIGRATE -98
#define DO_NOT_MIGRATE_DECISION                                                \
    SchedulingDecision(DO_NOT_MIGRATE, DO_NOT_MIGRATE)
#define NOT_ENOUGH_SLOTS -99
#define NOT_ENOUGH_SLOTS_DECISION                                              \
    SchedulingDecision(NOT_ENOUGH_SLOTS, NOT_ENOUGH_SLOTS)

namespace faabric::batch_scheduler {

typedef std::pair<std::shared_ptr<BatchExecuteRequest>,
                  std::shared_ptr<SchedulingDecision>>
  InFlightPair;

typedef std::map<int32_t, InFlightPair> InFlightReqs;

// TODO: remove duplication with PlannerState
struct HostState
{
    HostState(const std::string& ipIn, int slotsIn, int usedSlotsIn)
      : ip(ipIn)
      , slots(slotsIn)
      , usedSlots(usedSlotsIn)
    {}

    std::string ip;
    int slots;
    int usedSlots;
};
typedef std::shared_ptr<HostState> Host;
typedef std::map<std::string, Host> HostMap;

/*
 * The batch scheduler makes three different types of scheduling decisions.
 * 1) A `NEW` scheduling decision happens when we are scheduling a BER for the
 * first time.
 * 2) A `DIST_CHANGE` scheduling decision happens when we are scheduling a BER
 * _not_ for the first time, but the BER has the same number of messages that
 * it had before, and is set with the MIGRATION flag. This corresponds to a
 * request to migrate.
 * 3) A `SCALE_CHANGE` scheduling decision happens when we are scheduling a BER
 * _not_ for the first time, and the BER has a differet number of messages than
 * it had before. This corresponds to a chaining request or a thread/process
 * fork. IMPORTANT: in a `SCALE_CHANGE` decision, we indicate the NEW number of
 * messages we want to add to the running request, not the TOTAL
 */
enum DecisionType
{
    NO_DECISION_TYPE = 0,
    NEW = 1,
    DIST_CHANGE = 2,
    SCALE_CHANGE = 3,
};

/*
 * Interface class for different scheduler implementations to be used with the
 * planner.
 */
class BatchScheduler
{
  public:
    static DecisionType getDecisionType(
      const InFlightReqs& inFlightReqs,
      std::shared_ptr<faabric::BatchExecuteRequest> req);

    virtual std::shared_ptr<SchedulingDecision> makeSchedulingDecision(
      const HostMap& hostMap,
      const InFlightReqs& inFlightReqs,
      std::shared_ptr<faabric::BatchExecuteRequest> req) = 0;

  protected:
    // ----------
    // Helper Host accessor metods (we encapsulate them to allow changing the
    // underlying `Host` typedef easily)
    // TODO: consider moving to a separate class, or as members of the Host
    // wrapper class
    // ----------

    static int numSlots(const Host& host) { return host->slots; }

    static int numSlotsAvailable(const Host& host)
    {
        return std::max<int>(0, numSlots(host) - host->usedSlots);
    }

    static void claimSlots(Host& host, int numSlotsToClaim)
    {
        host->usedSlots =
          std::min<int>(numSlots(host), host->usedSlots + numSlotsToClaim);
    }

    static void freeSlots(Host& host, int numSlotsToFree)
    {
        host->usedSlots = std::max<int>(0, host->usedSlots - numSlotsToFree);
    }

    static std::string getIp(const Host& host) { return host->ip; }

    // ----------
    // Virtual scheduling methods
    // ----------

    virtual bool isFirstDecisionBetter(
      std::shared_ptr<SchedulingDecision> decisionA,
      std::shared_ptr<SchedulingDecision> decisionB) = 0;

    virtual std::vector<Host> getSortedHosts(
      const HostMap& hostMap,
      const InFlightReqs& inFlightReqs,
      std::shared_ptr<faabric::BatchExecuteRequest> req,
      const DecisionType& decisionType) = 0;
};

std::shared_ptr<BatchScheduler> getBatchScheduler();

void resetBatchScheduler();
}
