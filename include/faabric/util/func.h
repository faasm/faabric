#pragma once

#include <faabric/proto/faabric.pb.h>
#include <faabric/util/exception.h>
#include <string>
#include <vector>

namespace faabric::util {

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

std::shared_ptr<faabric::BatchExecuteRequest> batchExecFactory();

std::shared_ptr<faabric::BatchExecuteRequest> batchExecFactory(
  const std::string& user,
  const std::string& function,
  int count = 1);

std::string resultKeyFromMessageId(unsigned int mid);

std::string statusKeyFromMessageId(unsigned int mid);

std::vector<uint8_t> messageToBytes(const faabric::Message& msg);

std::vector<std::string> getArgvForMessage(const faabric::Message& msg);
}
