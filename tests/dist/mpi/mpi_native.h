#pragma once

#include <faabric/proto/faabric.pb.h>

namespace tests::mpi {

extern faabric::Message* executingCall;

// --- List of MPI functions ---

int allGather();

int allReduce();

int allToAll();

int broadcast();

int cartCreate();

int cartesian();

int checks();

int gather();

int helloWorld();

int iSendRecv();

int oneSided();

int order();

int probe();

int reduce();

int reduceMany();

int scan();

int scatter();

int send();

int sendMany();

int sendRecv();

int sendSyncAsync();

int status();

int typeSize();

int winCreate();
}
