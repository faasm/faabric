#pragma once

#include <proto/faabric.pb.h>
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

std::string execNodeToJson(const ExecGraphNode& node);

std::string execGraphToJson(const ExecGraph& graph);
}
