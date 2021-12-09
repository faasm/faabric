#include <faabric/scheduler/ExecGraph.h>

#include <faabric/util/json.h>
#include <sstream>

namespace faabric::scheduler {

int countExecGraphNode(const ExecGraphNode& node)
{
    int count = 1;

    if (!node.children.empty()) {
        for (auto c : node.children) {
            count += countExecGraphNode(c);
        }
    }

    return count;
}

int countExecGraphNodes(const ExecGraph& graph)
{
    ExecGraphNode rootNode = graph.rootNode;
    int count = countExecGraphNode(rootNode);
    return count;
}

std::set<std::string> getExecGraphHostsForNode(const ExecGraphNode& node)
{
    std::set<std::string> hostsForNode;
    hostsForNode.insert(node.msg.executedhost());

    for (auto c : node.children) {
        std::set<std::string> nodeHost = getExecGraphHostsForNode(c);
        hostsForNode.insert(nodeHost.begin(), nodeHost.end());
    }

    return hostsForNode;
}

std::set<std::string> getExecGraphHosts(const ExecGraph& graph)
{
    return getExecGraphHostsForNode(graph.rootNode);
}

// ----------------------------------------
// TODO - do this with RapidJson and not sstream
// ----------------------------------------

std::string execNodeToJson(const ExecGraphNode& node)
{
    std::stringstream res;

    // Add the message
    res << "{ \"msg\": " << faabric::util::messageToJson(node.msg);

    // Add the children
    if (!node.children.empty()) {
        res << ", " << std::endl << "\"chained\": [" << std::endl;

        for (unsigned int i = 0; i < node.children.size(); i++) {
            res << execNodeToJson(node.children.at(i));

            if (i < node.children.size() - 1) {
                res << ", " << std::endl;
            }
        }

        res << std::endl << "]";
    }

    res << "}";

    return res.str();
}

std::string execGraphToJson(const ExecGraph& graph)
{
    std::stringstream res;

    res << "{ " << std::endl
        << "\"root\": " << execNodeToJson(graph.rootNode) << std::endl
        << " }";

    return res.str();
}
}
