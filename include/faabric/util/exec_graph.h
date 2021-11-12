#pragma once

#include <faabric/proto/faabric.pb.h>

#include <functional>
#include <list>
#include <map>

namespace faabric::util::exec_graph {
void addDetail(faabric::Message& msg,
               const std::string& key,
               const std::string& value);

void incrementCounter(faabric::Message& msg,
                      const std::string& key,
                      const int valueToIncrement = 1);

static inline std::string const mpiMsgCountPrefix = "mpi-msgcount-torank-";

static inline std::string const mpiMsgTypeCountPrefix = "mpi-msgtype-torank";
}
