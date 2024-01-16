#pragma once

#include <faabric/proto/faabric.pb.h>

namespace faabric::executor {

class ExecutorTask
{
  public:
    ExecutorTask() = default;

    ExecutorTask(int messageIndexIn,
                 std::shared_ptr<BatchExecuteRequest> reqIn);

    // Delete everything copy-related, default everything move-related
    ExecutorTask(const ExecutorTask& other) = delete;

    ExecutorTask& operator=(const ExecutorTask& other) = delete;

    ExecutorTask(ExecutorTask&& other) = default;

    ExecutorTask& operator=(ExecutorTask&& other) = default;

    std::shared_ptr<BatchExecuteRequest> req;
    int messageIndex = 0;
};
}
