#pragma once

#include <faabric/proto/faabric.pb.h>
#include <faabric/util/exception.h>
#include <string>
#include <vector>

#define MIGRATED_FUNCTION_RETURN_VALUE -99

namespace faabric::util {

class FunctionMigratedException : public faabric::util::FaabricException
{
  public:
    explicit FunctionMigratedException(std::string message)
      : FaabricException(std::move(message))
    {}
};

std::string funcToString(const faabric::Message& msg, bool includeId);

std::string funcToString(
  const std::shared_ptr<faabric::BatchExecuteRequest>& req);

unsigned int setMessageId(faabric::Message& msg);

std::string buildAsyncResponse(const faabric::Message& msg);

std::shared_ptr<faabric::Message> messageFactoryShared(
  const std::string& user,
  const std::string& function);

faabric::Message messageFactory(const std::string& user,
                                const std::string& function);

std::string resultKeyFromMessageId(unsigned int mid);

std::string statusKeyFromMessageId(unsigned int mid);

std::vector<uint8_t> messageToBytes(const faabric::Message& msg);

std::vector<std::string> getArgvForMessage(const faabric::Message& msg);

/*
 * Gets the key for the main thread snapshot for the given message. Result will
 * be the same on all hosts.
 */
std::string getMainThreadSnapshotKey(const faabric::Message& msg);

}
