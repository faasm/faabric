#pragma once

#include <faabric/proto/faabric.pb.h>

#include <set>
#include <vector>

namespace faabric::scheduler {

struct ExecGraphNode
{
    faabric::Message msg;
    std::vector<ExecGraphNode> children;
};

struct ExecGraph
{
    ExecGraphNode rootNode;
};

int countExecGraphNodes(const ExecGraph& graph);

std::set<std::string> getExecGraphHosts(const ExecGraph& graph);

std::vector<std::string> getMpiRankHostsFromExecGraph(const ExecGraph& graph);

void getMigratedMpiRankHostsFromExecGraph(const ExecGraph& graph,
                                          std::vector<std::string>& hostsBefore,
                                          std::vector<std::string>& hostsAfter);

std::string execNodeToJson(const ExecGraphNode& node);

std::string execGraphToJson(const ExecGraph& graph);
}
