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

std::pair<std::vector<std::string>, std::vector<std::string>>
getMigratedMpiRankHostsFromExecGraph(const ExecGraph& graph);

std::string execNodeToJson(const ExecGraphNode& node);

std::string execGraphToJson(const ExecGraph& graph);
}
