#pragma once

#include <faabric/proto/faabric.pb.h>

namespace faabric::util {
std::shared_ptr<faabric::BatchExecuteRequest> batchExecFactory();

std::shared_ptr<faabric::BatchExecuteRequest> batchExecFactory(
  const std::string& user,
  const std::string& function,
  int count = 1);

bool isBatchExecRequestValid(std::shared_ptr<faabric::BatchExecuteRequest> ber);
}
