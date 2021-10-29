#include <faabric/util/exec_graph.h>
#include <faabric/util/logging.h>
#include <faabric/util/testing.h>

namespace faabric::util::exec_graph {
void addDetail(faabric::Message& msg,
               const std::string& key,
               const std::string& value)
{
    auto& stringMap = *msg.mutable_execgraphdetails();

    stringMap[key] = value;
}

void incrementCounter(faabric::Message& msg,
                      const std::string& key,
                      const int valueToIncrement)
{
    auto& stringMap = *msg.mutable_intexecgraphdetails();

    stringMap[key] += valueToIncrement;
}
}
