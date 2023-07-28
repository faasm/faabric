#include <faabric/planner/PlannerApi.h>
#include <faabric/planner/PlannerClient.h>
#include <faabric/planner/planner.pb.h>
#include <faabric/transport/common.h>
#include <faabric/util/concurrent_map.h>
#include <faabric/util/config.h>
#include <faabric/util/locks.h>
#include <faabric/util/logging.h>
#include <faabric/util/network.h>

namespace faabric::planner {

// -----------------------------------
// Keep-Alive Thread
// -----------------------------------

void KeepAliveThread::doWork()
{
    PlannerClient cli;

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
    faabric::EmptyResponse response;
    syncSend(PlannerCalls::SetMessageResult, msg.get(), &response);
}

// This function sets a message result locally. It is invoked as a callback
// from the planner to notify all hosts waiting for a message result that the
// result is ready
void PlannerClient::setMessageResultLocally(
  std::shared_ptr<faabric::Message> msg)
{
    faabric::util::UniqueLock lock(plannerResultsMutex);

    // It may happen that the planner returns the message result before we
    // have had time to prepare the promise. This should happen rarely as it
    // is an unexpected race condition, thus why we emit a warning
    if (plannerResults.find(msg->id()) == plannerResults.end()) {
        SPDLOG_WARN(
          "Setting message result before promise is set for (id: {}, app: {})",
          msg->id(),
          msg->appid());
        plannerResults.insert(
          { msg->id(), std::make_shared<MessageResultPromise>() });
    }

    plannerResults.at(msg->id())->set_value(msg);
}

// This method actually gets the message result from the planner
std::shared_ptr<faabric::Message> PlannerClient::getMessageResult(
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
    // Deliberately make a copy here so that we can set the masterhost when
    // registering interest in the results
    auto msgPtr = std::make_shared<faabric::Message>(msg);
    msgPtr->set_masterhost(faabric::util::getSystemConfig().endpointHost);
    return doGetFunctionResult(msgPtr, timeoutMs);
}

faabric::Message PlannerClient::getMessageResult(int appId,
                                                 int msgId,
                                                 int timeoutMs)
{
    auto msgPtr = std::make_shared<faabric::Message>();
    msgPtr->set_appid(appId);
    msgPtr->set_id(msgId);
    msgPtr->set_masterhost(faabric::util::getSystemConfig().endpointHost);
    return doGetFunctionResult(msgPtr, timeoutMs);
}

// This method gets the function result from the planner in a blocking fashion.
// Even though the results are stored in the planner, we want to block in the
// client (i.e. here) and not in the planner. This is to avoid consuming
// planner threads. This method will first query the planner once
// for the result. If its not there, the planner will register this host's
// interest, and send a function call setting the message result. In the
// meantime, we wait on a promise
faabric::Message PlannerClient::doGetFunctionResult(
  std::shared_ptr<faabric::Message> msgPtr,
  int timeoutMs)
{
    int msgId = msgPtr->id();
    auto resMsgPtr =
      faabric::planner::getPlannerClient()->getMessageResult(msgPtr);

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
        faabric::util::UniqueLock lock(plannerResultsMutex);

        if (plannerResults.find(msgId) == plannerResults.end()) {
            plannerResults.insert(
              { msgId, std::make_shared<MessageResultPromise>() });
        }

        // Note that it is deliberately an error for two threads to retrieve
        // the future at the same time
        fut = plannerResults.at(msgId)->get_future();
    }

    while (true) {
        faabric::Message msgResult;
        auto status = fut.wait_for(std::chrono::milliseconds(timeoutMs));
        if (status == std::future_status::timeout) {
            msgResult.set_type(faabric::Message_MessageType_EMPTY);
        } else {
            // Acquire a lock to read the value of the promise to avoid data
            // races
            faabric::util::UniqueLock lock(plannerResultsMutex);
            msgResult = *fut.get();
        }

        {
            // Remove the result promise irrespective of whether we timed out
            // or not, as promises are single-use
            faabric::util::UniqueLock lock(plannerResultsMutex);
            plannerResults.erase(msgId);
        }

        return msgResult;
    }
}

// -----------------------------------
// Static setter/getters
// -----------------------------------

// Even though there's just one planner server, and thus there will only be
// one client per instance, using a ConcurrentMap gives us the thread-safe
// wrapper for free
static faabric::util::
  ConcurrentMap<std::string, std::shared_ptr<faabric::planner::PlannerClient>>
    plannerClient;

std::shared_ptr<faabric::planner::PlannerClient> getPlannerClient()
{
    auto plannerHost = faabric::util::getIPFromHostname(
      faabric::util::getSystemConfig().plannerHost);
    auto client = plannerClient.get(plannerHost).value_or(nullptr);
    if (client == nullptr) {
        SPDLOG_DEBUG("Adding new planner client for {}", plannerHost);
        client = plannerClient.tryEmplaceShared(plannerHost).second;
    }
    return client;
}

void clearPlannerClient()
{
    plannerClient.clear();
}
}
