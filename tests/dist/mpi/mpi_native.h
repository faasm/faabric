#pragma once

#include <faabric/proto/faabric.pb.h>

namespace tests {

extern faabric::Message* executingCall;

// --- List of MPI functions ---

int allGather();

int allReduce();

int allToAll();

int broadcast();

int cartCreate();

int cartesian();

int checks();
}
