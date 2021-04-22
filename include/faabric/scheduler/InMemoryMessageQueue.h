#pragma once

#include <unordered_map>

#include "faabric/proto/faabric.pb.h"
#include <faabric/util/func.h>
#include <faabric/util/queue.h>

namespace faabric::scheduler {
typedef std::pair<int, std::shared_ptr<faabric::BatchExecuteRequest>>
  MessageTask;

typedef faabric::util::Queue<MessageTask> InMemoryBatchQueue;

typedef std::pair<std::string, std::shared_ptr<InMemoryBatchQueue>>
  InMemoryBatchQueuePair;

typedef std::unordered_map<std::string, std::shared_ptr<InMemoryBatchQueue>>
  InMemoryBatchQueueMap;

typedef faabric::util::Queue<faabric::Message> InMemoryMessageQueue;
typedef std::pair<std::string, InMemoryMessageQueue*> InMemoryMessageQueuePair;
}
