#include <faabric/proto/faabric.pb.h>
#include <faabric/scheduler/Scheduler.h>
#include <faabric/transport/PointToPointCall.h>
#include <faabric/transport/PointToPointClient.h>
#include <faabric/transport/PointToPointMessage.h>
#include <faabric/transport/common.h>
#include <faabric/transport/macros.h>
#include <faabric/util/testing.h>

namespace faabric::transport {

static std::mutex mockMutex;

static std::vector<std::pair<std::string, faabric::PointToPointMappings>>
  sentMappings;

static std::vector<std::pair<std::string, PointToPointMessage>> sentMessages;

static std::vector<std::tuple<std::string,
                              faabric::transport::PointToPointCall,
                              PointToPointMessage>>
  sentLockMessages;

std::vector<std::pair<std::string, faabric::PointToPointMappings>>
getSentMappings()
{
    return sentMappings;
}

std::vector<std::pair<std::string, PointToPointMessage>>
getSentPointToPointMessages()
{
    return sentMessages;
}

std::vector<std::tuple<std::string,
                       faabric::transport::PointToPointCall,
                       PointToPointMessage>>
getSentLockMessages()
{
    return sentLockMessages;
}

void clearSentMessages()
{
    sentMappings.clear();
    sentMessages.clear();
    sentLockMessages.clear();
}

PointToPointClient::PointToPointClient(const std::string& hostIn)
  : faabric::transport::MessageEndpointClient(hostIn,
                                              POINT_TO_POINT_ASYNC_PORT,
                                              POINT_TO_POINT_SYNC_PORT)
{}

void PointToPointClient::sendMappings(faabric::PointToPointMappings& mappings)
{
    if (faabric::util::isMockMode()) {
        sentMappings.emplace_back(host, mappings);
    } else {
        faabric::EmptyResponse resp;
        syncSend(PointToPointCall::MAPPING, &mappings, &resp);
    }
}

void PointToPointClient::sendMessage(const PointToPointMessage& msg,
                                     int sequenceNum)
{
    if (faabric::util::isMockMode()) {
        sentMessages.emplace_back(host, msg);
    } else {
        // TODO(FIXME): consider how we can avoid serialising once, and then
        // copying again into NNG's buffer
        std::vector<uint8_t> buffer(sizeof(msg) + msg.dataSize);
        serializePtpMsg(buffer, msg);
        asyncSend(
          PointToPointCall::MESSAGE, buffer.data(), buffer.size(), sequenceNum);
    }
}

void PointToPointClient::makeCoordinationRequest(
  int appId,
  int groupId,
  int groupIdx,
  faabric::transport::PointToPointCall call)
{
    PointToPointMessage req({ .appId = appId,
                              .groupId = groupId,
                              .sendIdx = groupIdx,
                              .recvIdx = POINT_TO_POINT_MAIN_IDX,
                              .dataSize = 0,
                              .dataPtr = nullptr });

    switch (call) {
        case (faabric::transport::PointToPointCall::LOCK_GROUP): {
            SPDLOG_TRACE("Requesting lock on {} at {}", groupId, host);
            break;
        }
        case (faabric::transport::PointToPointCall::LOCK_GROUP_RECURSIVE): {
            SPDLOG_TRACE(
              "Requesting recursive lock on {} at {}", groupId, host);
            break;
        }
        case (faabric::transport::PointToPointCall::UNLOCK_GROUP): {
            SPDLOG_TRACE("Requesting unlock on {} at {}", groupId, host);
            break;
        }
        case (faabric::transport::PointToPointCall::UNLOCK_GROUP_RECURSIVE): {
            SPDLOG_TRACE(
              "Requesting recurisve unlock on {} at {}", groupId, host);
            break;
        }
        default: {
            SPDLOG_ERROR("Invalid function group call {}", call);
            throw std::runtime_error("Invalid function group call");
        }
    }

    if (faabric::util::isMockMode()) {
        faabric::util::UniqueLock lock(mockMutex);
        sentLockMessages.emplace_back(host, call, req);
    } else {
        // TODO(FIXME): consider how we can avoid serialising once, and then
        // copying again into NNG's buffer
        std::vector<uint8_t> buffer(sizeof(PointToPointMessage) + req.dataSize);
        serializePtpMsg(buffer, req);
        asyncSend(call, buffer.data(), buffer.size());
    }
}

void PointToPointClient::groupLock(int appId,
                                   int groupId,
                                   int groupIdx,
                                   bool recursive)
{
    makeCoordinationRequest(appId,
                            groupId,
                            groupIdx,
                            recursive ? PointToPointCall::LOCK_GROUP_RECURSIVE
                                      : PointToPointCall::LOCK_GROUP);
}

void PointToPointClient::groupUnlock(int appId,
                                     int groupId,
                                     int groupIdx,
                                     bool recursive)
{
    makeCoordinationRequest(appId,
                            groupId,
                            groupIdx,
                            recursive ? PointToPointCall::UNLOCK_GROUP_RECURSIVE
                                      : PointToPointCall::UNLOCK_GROUP);
}
}
