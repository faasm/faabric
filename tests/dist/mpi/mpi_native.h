#pragma once

#include <atomic>
#include <faabric/proto/faabric.pb.h>

#define NUM_MIGRATION_LOOPS 10000
#define CHECK_EVERY 5000

namespace tests::mpi {

// --- List of MPI functions ---

int allGather();

int allReduce();

int allToAll();

int allToAllAndSleep();

int barrier();

int broadcast();

int cartCreate();

int cartesian();

int checks();

int gather();

int helloWorld();

int iSendRecv();

int migration(int nLoops);

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

// Other helper functions
int doAllToAll(int rank, int worldSize, int i);

int bench_allreduce();

void mpiMigrationPoint(int entrypointFuncArg);
}
