#include <faabric/scheduler/ExecGraph.h>
#include <faabric/scheduler/Scheduler.h>

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

std::vector<std::string> getMpiRankHostsFromExecGraphNode(
  const ExecGraphNode& node)
{
    std::vector<std::string> rankHosts(node.msg.mpiworldsize());
    int rank = node.msg.mpirank();
    std::string host = node.msg.executedhost();
    rankHosts.at(rank) = host;

    for (auto c : node.children) {
        std::vector<std::string> nodeRankHosts =
          getMpiRankHostsFromExecGraphNode(c);
        assert(nodeRankHosts.size() == rankHosts.size());
        for (int i = 0; i < rankHosts.size(); i++) {
            if (!nodeRankHosts.at(i).empty()) {
                rankHosts.at(i) = nodeRankHosts.at(i);
            }
        }
    }

    return rankHosts;
}

std::vector<std::string> getMpiRankHostsFromExecGraph(const ExecGraph& graph)
{
    return getMpiRankHostsFromExecGraphNode(graph.rootNode);
}

void getMigratedMpiRankHostsFromExecGraph(const ExecGraph& graph,
                                          std::vector<std::string>& hostsBefore,
                                          std::vector<std::string>& hostsAfter)
{
    std::queue<faabric::scheduler::ExecGraphNode> nodeList;
    nodeList.push(graph.rootNode);
    while (!nodeList.empty()) {
        // Process the node at the front
        auto node = nodeList.front();
        int returnValue = node.msg.returnvalue();
        int rank = node.msg.mpirank();
        std::string executedHost = node.msg.executedhost();
        if (returnValue == 0) {
            // We don't know if this particular rank has been migrated or
            // not. Thus we only write in the before vector if no-one has
            // written to that rank before
            if (hostsBefore.at(rank).empty()) {
                hostsBefore.at(rank) = executedHost;
            }
            hostsAfter.at(rank) = executedHost;
        } else if (returnValue == MIGRATED_FUNCTION_RETURN_VALUE) {
            // When we process a message that has been migrated we always
            // overwrite the contents of the before vector
            hostsBefore.at(rank) = executedHost;
        } else {
            SPDLOG_ERROR("Unexpected return value {} for message id {}",
                         returnValue,
                         node.msg.id());
            throw std::runtime_error("Unexpected return value");
        }
        nodeList.pop();

        // Add children to the queue
        for (auto c : node.children) {
            nodeList.push(c);
        }
    }
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
