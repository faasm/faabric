#pragma once

#include <faabric/proto/faabric.pb.h>

namespace faabric::util {

// ----------
// Batch Execute Requests (BER)
// ----------

std::shared_ptr<faabric::BatchExecuteRequest> batchExecFactory();

std::shared_ptr<faabric::BatchExecuteRequest> batchExecFactory(
  const std::string& user,
  const std::string& function,
  int count = 1);

bool isBatchExecRequestValid(std::shared_ptr<faabric::BatchExecuteRequest> ber);

void updateBatchExecGroupId(std::shared_ptr<faabric::BatchExecuteRequest> ber,
                            int newGroupId);

// ----------
// Batch Execute Requests' Status
// ----------

std::shared_ptr<faabric::BatchExecuteRequestStatus> batchExecStatusFactory(
  int32_t appId);

std::shared_ptr<faabric::BatchExecuteRequestStatus> batchExecStatusFactory(
  std::shared_ptr<faabric::BatchExecuteRequest> ber);
}
