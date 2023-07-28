#include <faabric/scheduler/Scheduler.h>
#include <faabric/util/ExecGraph.h>
#include <faabric/util/json.h>
#include <faabric/util/logging.h>

#include <queue>
#include <sstream>

#define EXEC_GRAPH_TIMEOUT_MS 1000
// TODO: avoid this duplication
#define MIGRATED_FUNCTION_RETURN_VALUE -99

namespace faabric::util {

ExecGraph getFunctionExecGraph(const faabric::Message& msg)
{
    ExecGraphNode rootNode;
    try {
        rootNode = getFunctionExecGraphNode(msg.appid(), msg.id());
    } catch (ExecGraphNodeNotFoundException& e) {
        return ExecGraph{};
    }

    faabric::util::ExecGraph graph{ .rootNode = rootNode };

    return graph;
}

ExecGraphNode getFunctionExecGraphNode(int appId, int msgId)
{
    // We build a new message from scratch everytime here. We could avoid it if
    // it becomes a bottleneck
    faabric::Message msg;
    msg.set_id(msgId);
    msg.set_appid(appId);
    // Get result without blocking, as we expect the results to have been
    // published already
    faabric::Message resultMsg =
      faabric::planner::getPlannerClient().getMessageResult(msg, 0);
    if (resultMsg.type() == faabric::Message_MessageType_EMPTY) {
        SPDLOG_ERROR(
          "Message result in exec. graph not ready (msg: {} - app: {})",
          msgId,
          appId);
        throw ExecGraphNodeNotFoundException("Exec. Graph node not ready!");
    }

    // Recurse through chained calls
    std::set<unsigned int> chainedMsgIds(
      resultMsg.mutable_chainedmsgids()->begin(),
      resultMsg.mutable_chainedmsgids()->end());
    std::vector<ExecGraphNode> children;
    for (auto chainedMsgId : chainedMsgIds) {
        children.emplace_back(getFunctionExecGraphNode(appId, chainedMsgId));
    }

    // Build the node
    ExecGraphNode node{ .msg = resultMsg, .children = children };

    return node;
}

void logChainedFunction(faabric::Message& parentMessage,
                        const faabric::Message& chainedMessage)
{
    parentMessage.add_chainedmsgids(chainedMessage.id());
}

std::set<unsigned int> getChainedFunctions(const faabric::Message& msg)
{
    // Note that we can't get the chained functions until the result for the
    // parent message has been set
    auto resultMsg = faabric::planner::getPlannerClient().getMessageResult(
      msg, EXEC_GRAPH_TIMEOUT_MS);
    std::set<unsigned int> chainedIds(
      resultMsg.mutable_chainedmsgids()->begin(),
      resultMsg.mutable_chainedmsgids()->end());

    return chainedIds;
}

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

std::pair<std::vector<std::string>, std::vector<std::string>>
getMigratedMpiRankHostsFromExecGraph(const ExecGraph& graph)
{
    // Initialise return vectors
    std::vector<std::string> hostsBefore(graph.rootNode.msg.mpiworldsize());
    std::vector<std::string> hostsAfter(graph.rootNode.msg.mpiworldsize());

    std::queue<ExecGraphNode> nodeList;
    nodeList.push(graph.rootNode);

    // Instead of iterating the execution graph recursively, we do it
    // sequentially as it is easier to populate the two returned vectors
    while (!nodeList.empty()) {
        auto node = nodeList.front();
        int returnValue = node.msg.returnvalue();
        int rank = node.msg.mpirank();
        std::string executedHost = node.msg.executedhost();

        // Each function in the execution graph (i.e. MPI rank) that has
        // finished succesfully has either been migrated or not. Each migrated
        // rank accounts for two functions: one that has been stopped to be
        // migrated and one that has finished succesfully.
        if (returnValue == 0) {
            // If the function has finished succesfully it is either the second
            // function for the same rank (i.e. it has been migrated) or not
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

    return std::make_pair(hostsBefore, hostsAfter);
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

void addDetail(faabric::Message& msg,
               const std::string& key,
               const std::string& value)
{
    if (!msg.recordexecgraph()) {
        return;
    }

    auto& stringMap = *msg.mutable_execgraphdetails();

    stringMap[key] = value;
}

void incrementCounter(faabric::Message& msg,
                      const std::string& key,
                      const int valueToIncrement)
{
    if (!msg.recordexecgraph()) {
        return;
    }

    auto& stringMap = *msg.mutable_intexecgraphdetails();

    stringMap[key] += valueToIncrement;
}
}
