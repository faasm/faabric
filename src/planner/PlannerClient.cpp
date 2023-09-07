#include <faabric/planner/PlannerApi.h>
#include <faabric/planner/PlannerClient.h>
#include <faabric/planner/planner.pb.h>
#include <faabric/snapshot/SnapshotClient.h>
#include <faabric/snapshot/SnapshotRegistry.h>
#include <faabric/transport/common.h>
#include <faabric/util/batch.h>
#include <faabric/util/concurrent_map.h>
#include <faabric/util/config.h>
#include <faabric/util/func.h>
#include <faabric/util/locks.h>
#include <faabric/util/logging.h>
#include <faabric/util/network.h>

namespace faabric::planner {

// -----------------------------------
// Keep-Alive Thread
// -----------------------------------

void KeepAliveThread::doWork()
{
    auto& cli = getPlannerClient();

    faabric::util::SharedLock lock(keepAliveThreadMx);

    cli.registerHost(thisHostReq);
}

void KeepAliveThread::setRequest(
  std::shared_ptr<RegisterHostRequest> thisHostReqIn)
{
    faabric::util::FullLock lock(keepAliveThreadMx);

    thisHostReq = thisHostReqIn;

    // Keep-alive requests should never overwrite the state of the planner
    thisHostReq->set_overwrite(false);
}

// -----------------------------------
// Planner Client
// -----------------------------------

PlannerClient::PlannerClient()
  : faabric::transport::MessageEndpointClient(
      faabric::util::getIPFromHostname(
        faabric::util::getSystemConfig().plannerHost),
      PLANNER_ASYNC_PORT,
      PLANNER_SYNC_PORT)
{}

PlannerClient::PlannerClient(const std::string& plannerIp)
  : faabric::transport::MessageEndpointClient(plannerIp,
                                              PLANNER_ASYNC_PORT,
                                              PLANNER_SYNC_PORT)
  , snapshotClient(faabric::snapshot::getSnapshotClient(plannerIp))
{}

void PlannerClient::ping()
{
    EmptyRequest req;
    PingResponse resp;
    syncSend(PlannerCalls::Ping, &req, &resp);

    // Sanity check
    auto expectedIp = faabric::util::getIPFromHostname(
      faabric::util::getSystemConfig().plannerHost);
    auto gotIp = resp.config().ip();
    // In a k8s deployment, where the planner pod is deployed behind a service,
    // expectedIp will correspond to the service IP, and gotIp to the pod's
    // one (i.e. as reported by the planner locally getting its endpoint host)
    // I don't consider it an error that they differ, and the worker should
    // use the service one
    if (expectedIp != gotIp) {
        SPDLOG_DEBUG(
          "Got two IPs pinging the planner server (our ip: {} - their {})",
          expectedIp,
          gotIp);
    }

    SPDLOG_DEBUG("Succesfully pinged the planner server at {}", expectedIp);
}

void PlannerClient::clearCache()
{
    faabric::util::UniqueLock lock(plannerCacheMx);

    cache.plannerResults.clear();
}

std::vector<faabric::planner::Host> PlannerClient::getAvailableHosts()
{
    EmptyRequest req;
    AvailableHostsResponse resp;
    syncSend(PlannerCalls::GetAvailableHosts, &req, &resp);

    // Copy the repeated array into a more convinient container
    std::vector<Host> availableHosts;
    for (int i = 0; i < resp.hosts_size(); i++) {
        availableHosts.push_back(resp.hosts(i));
    }

    return availableHosts;
}

int PlannerClient::registerHost(std::shared_ptr<RegisterHostRequest> req)
{
    RegisterHostResponse resp;
    syncSend(PlannerCalls::RegisterHost, req.get(), &resp);

    if (resp.status().status() != ResponseStatus_Status_OK) {
        throw std::runtime_error("Error registering host with planner!");
    }

    // Sanity check
    assert(resp.config().hosttimeout() > 0);

    // Return the planner's timeout to set the keep-alive heartbeat
    return resp.config().hosttimeout();
}

void PlannerClient::removeHost(std::shared_ptr<RemoveHostRequest> req)
{
    faabric::EmptyResponse response;
    syncSend(PlannerCalls::RemoveHost, req.get(), &response);
}

void PlannerClient::setMessageResult(std::shared_ptr<faabric::Message> msg)
{
    asyncSend(PlannerCalls::SetMessageResult, msg.get());
}

// This function sets a message result locally. It is invoked as a callback
// from the planner to notify all hosts waiting for a message result that the
// result is ready
void PlannerClient::setMessageResultLocally(
  std::shared_ptr<faabric::Message> msg)
{
    faabric::util::UniqueLock lock(plannerCacheMx);

    // It may happen that the planner returns the message result before we
    // have had time to prepare the promise. This should happen rarely as it
    // is an unexpected race condition, thus why we emit a warning
    if (cache.plannerResults.find(msg->id()) == cache.plannerResults.end()) {
        SPDLOG_WARN(
          "Setting message result before promise is set for (id: {}, app: {})",
          msg->id(),
          msg->appid());
        cache.plannerResults.insert(
          { msg->id(), std::make_shared<MessageResultPromise>() });
    }

    cache.plannerResults.at(msg->id())->set_value(msg);
}

// This internal method method actually gets the message result from the
// planner
std::shared_ptr<faabric::Message> PlannerClient::getMessageResultFromPlanner(
  std::shared_ptr<faabric::Message> msg)
{
    faabric::Message responseMsg;
    syncSend(PlannerCalls::GetMessageResult, msg.get(), &responseMsg);

    if (responseMsg.id() == 0 && responseMsg.appid() == 0) {
        return nullptr;
    }

    return std::make_shared<faabric::Message>(responseMsg);
}

faabric::Message PlannerClient::getMessageResult(const faabric::Message& msg,
                                                 int timeoutMs)
{
    // Deliberately make a copy here so that we can set the main host when
    // registering interest in the results
    auto msgPtr = std::make_shared<faabric::Message>(msg);
    msgPtr->set_mainhost(faabric::util::getSystemConfig().endpointHost);
    return doGetMessageResult(msgPtr, timeoutMs);
}

faabric::Message PlannerClient::getMessageResult(int appId,
                                                 int msgId,
                                                 int timeoutMs)
{
    auto msgPtr = std::make_shared<faabric::Message>();
    msgPtr->set_appid(appId);
    msgPtr->set_id(msgId);
    msgPtr->set_mainhost(faabric::util::getSystemConfig().endpointHost);
    return doGetMessageResult(msgPtr, timeoutMs);
}

// This method gets the function result from the planner in a blocking fashion.
// Even though the results are stored in the planner, we want to block in the
// client (i.e. here) and not in the planner. This is to avoid consuming
// planner threads. This method will first query the planner once
// for the result. If its not there, the planner will register this host's
// interest, and send a function call setting the message result. In the
// meantime, we wait on a promise
faabric::Message PlannerClient::doGetMessageResult(
  std::shared_ptr<faabric::Message> msgPtr,
  int timeoutMs)
{
    int msgId = msgPtr->id();
    auto resMsgPtr = getMessageResultFromPlanner(msgPtr);

    // If when we first check the message it is there, return. Otherwise, we
    // will have told the planner we want the result
    if (resMsgPtr) {
        return *resMsgPtr;
    }

    bool isBlocking = timeoutMs > 0;
    // If the result is not in the planner, and we are not blocking, return
    if (!isBlocking) {
        faabric::Message msgResult;
        msgResult.set_type(faabric::Message_MessageType_EMPTY);
        return msgResult;
    }

    // If we are here, we need to wait for the planner to let us know that
    // the message is ready. To do so, we need to set a promise at the message
    // id. We do so immediately after returning, so that we don't race with
    // the planner sending the result back
    std::future<std::shared_ptr<faabric::Message>> fut;
    {
        faabric::util::UniqueLock lock(plannerCacheMx);

        if (cache.plannerResults.find(msgId) == cache.plannerResults.end()) {
            cache.plannerResults.insert(
              { msgId, std::make_shared<MessageResultPromise>() });
        }

        // Note that it is deliberately an error for two threads to retrieve
        // the future at the same time
        fut = cache.plannerResults.at(msgId)->get_future();
    }

    while (true) {
        faabric::Message msgResult;
        auto status = fut.wait_for(std::chrono::milliseconds(timeoutMs));
        if (status == std::future_status::timeout) {
            msgResult.set_type(faabric::Message_MessageType_EMPTY);
        } else {
            // Acquire a lock to read the value of the promise to avoid data
            // races
            faabric::util::UniqueLock lock(plannerCacheMx);
            msgResult = *fut.get();
        }

        {
            // Remove the result promise irrespective of whether we timed out
            // or not, as promises are single-use
            faabric::util::UniqueLock lock(plannerCacheMx);
            cache.plannerResults.erase(msgId);
        }

        return msgResult;
    }
}

// This method is deliberately non-blocking, and returns all results tracked
// for a particular app
std::shared_ptr<faabric::BatchExecuteRequestStatus>
PlannerClient::getBatchResults(
  std::shared_ptr<faabric::BatchExecuteRequest> req)
{
    faabric::BatchExecuteRequestStatus berStatus;
    syncSend(PlannerCalls::GetBatchResults, req.get(), &berStatus);

    return std::make_shared<BatchExecuteRequestStatus>(berStatus);
}

faabric::batch_scheduler::SchedulingDecision PlannerClient::callFunctions(
  std::shared_ptr<faabric::BatchExecuteRequest> req)
{
    // ------------------------
    // THREADS
    // ------------------------

    // For threads, we need to indicate that keep track of the main host (i.e.
    // the host from which we invoke threads) to gather the diffs after
    // execution
    bool isThreads = req->type() == faabric::BatchExecuteRequest::THREADS;
    if (isThreads) {
        for (int i = 0; i < req->messages_size(); i++) {
            req->mutable_messages(i)->set_mainhost(
              faabric::util::getSystemConfig().endpointHost);
        }
    }

    // ------------------------
    // SNAPSHOTS
    // ------------------------

    // If we set a snapshot key (mostly in a threaded execution) we send the
    // snapshot to the planner, that will manage its lifecycle and distribution
    // to other hosts. Given that we don't support nested threading, if we
    // have a THREADS request here it means that we are being called from the
    // main thread (which holds the main snapshot)
    const std::string funcStr =
      faabric::util::funcToString(req->messages(0), false);
    auto& reg = faabric::snapshot::getSnapshotRegistry();

    std::string snapshotKey;
    const auto firstMsg = req->messages(0);
    if (isThreads) {
        if (!firstMsg.snapshotkey().empty()) {
            SPDLOG_ERROR("{} should not provide snapshot key for {} threads",
                         funcStr,
                         req->messages().size());

            std::runtime_error("Should not provide snapshot key for threads");
        }

        // To optimise for single-host shared memory, we can skip sending the
        // snapshot to the planner by setting the singlehost flag
        if (!req->singlehost()) {
            snapshotKey = faabric::util::getMainThreadSnapshotKey(firstMsg);
        }
    } else {
        // In a single-host setting we can skip sending the snapshots to the
        // planner
        if (!req->singlehost()) {
            snapshotKey = req->messages(0).snapshotkey();
        }
    }

    if (!snapshotKey.empty()) {
        faabric::util::UniqueLock lock(plannerCacheMx);
        auto snap = reg.getSnapshot(snapshotKey);

        // See if we've already pushed this snapshot to the planner once,
        // if so, just push the diffs that have occurred in this main thread
        if (cache.pushedSnapshots.contains(snapshotKey)) {
            std::vector<faabric::util::SnapshotDiff> snapshotDiffs =
              snap->getTrackedChanges();

            snapshotClient->pushSnapshotUpdate(
              snapshotKey, snap, snapshotDiffs);
        } else {
            snapshotClient->pushSnapshot(snapshotKey, snap);
            cache.pushedSnapshots.insert(snapshotKey);
        }

        // Now reset the tracking on the snapshot before we start executing
        snap->clearTrackedChanges();
    }

    // ------------------------
    // EXECUTION REQUEST TO PLANNER
    // ------------------------

    faabric::PointToPointMappings response;
    syncSend(PlannerCalls::CallBatch, req.get(), &response);

    auto decision =
      faabric::batch_scheduler::SchedulingDecision::fromPointToPointMappings(
        response);

    // The planner decision sets a group id for PTP communication. Make sure we
    // propagate the group id to the messages in the request. The group idx
    // is set when creating the request
    faabric::util::updateBatchExecGroupId(req, decision.groupId);

    return decision;
}

faabric::batch_scheduler::SchedulingDecision
PlannerClient::getSchedulingDecision(
  std::shared_ptr<faabric::BatchExecuteRequest> req)
{
    faabric::PointToPointMappings response;
    syncSend(PlannerCalls::GetSchedulingDecision, req.get(), &response);

    auto decision =
      faabric::batch_scheduler::SchedulingDecision::fromPointToPointMappings(
        response);

    return decision;
}

// -----------------------------------
// Static setter/getters
// -----------------------------------

PlannerClient& getPlannerClient()
{
    auto plannerHost = faabric::util::getIPFromHostname(
      faabric::util::getSystemConfig().plannerHost);

    static PlannerClient plannerCli(plannerHost);

    return plannerCli;
}
}
