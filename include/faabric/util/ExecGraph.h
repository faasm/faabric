#pragma once

#include <faabric/proto/faabric.pb.h>
#include <faabric/util/exception.h>

#include <set>
#include <vector>

namespace faabric::util {

class ExecGraphNodeNotFoundException : public faabric::util::FaabricException
{
  public:
    explicit ExecGraphNodeNotFoundException(std::string message)
      : FaabricException(std::move(message))
    {}
};

struct ExecGraphNode
{
    faabric::Message msg;
    std::vector<ExecGraphNode> children;
};

struct ExecGraph
{
    ExecGraphNode rootNode;
};

void logChainedFunction(faabric::Message& parentMessage,
                        const faabric::Message& chainedMessage);

std::set<unsigned int> getChainedFunctions(const faabric::Message& msg);

int countExecGraphNodes(const ExecGraph& graph);

std::set<std::string> getExecGraphHosts(const ExecGraph& graph);

std::vector<std::string> getMpiRankHostsFromExecGraph(const ExecGraph& graph);

std::pair<std::vector<std::string>, std::vector<std::string>>
getMigratedMpiRankHostsFromExecGraph(const ExecGraph& graph);

std::string execNodeToJson(const ExecGraphNode& node);

std::string execGraphToJson(const ExecGraph& graph);

void addDetail(faabric::Message& msg,
               const std::string& key,
               const std::string& value);

void incrementCounter(faabric::Message& msg,
                      const std::string& key,
                      const int valueToIncrement = 1);
}
