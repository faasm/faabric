#include <catch2/catch.hpp>

#include "DistTestExecutor.h"
#include "init.h"
#include "mpi/mpi_native.h"

#include <faabric/proto/faabric.pb.h>
#include <faabric/util/logging.h>

using namespace tests::mpi;

namespace tests {

int handleMpiBenchAllReduce(tests::DistTestExecutor* exec,
                            int threadPoolIdx,
                            int msgIdx,
                            std::shared_ptr<faabric::BatchExecuteRequest> req)
{
    return bench_allreduce();
}

int handleMpiAllGather(tests::DistTestExecutor* exec,
                       int threadPoolIdx,
                       int msgIdx,
                       std::shared_ptr<faabric::BatchExecuteRequest> req)
{
    return allGather();
}

int handleMpiAllReduce(tests::DistTestExecutor* exec,
                       int threadPoolIdx,
                       int msgIdx,
                       std::shared_ptr<faabric::BatchExecuteRequest> req)
{
    return allReduce();
}

int handleMpiAllToAll(tests::DistTestExecutor* exec,
                      int threadPoolIdx,
                      int msgIdx,
                      std::shared_ptr<faabric::BatchExecuteRequest> req)
{
    return allToAll();
}

int handleMpiAllToAllAndSleep(tests::DistTestExecutor* exec,
                              int threadPoolIdx,
                              int msgIdx,
                              std::shared_ptr<faabric::BatchExecuteRequest> req)
{
    return allToAllAndSleep();
}

int handleMpiBarrier(tests::DistTestExecutor* exec,
                     int threadPoolIdx,
                     int msgIdx,
                     std::shared_ptr<faabric::BatchExecuteRequest> req)
{
    return barrier();
}

int handleMpiBcast(tests::DistTestExecutor* exec,
                   int threadPoolIdx,
                   int msgIdx,
                   std::shared_ptr<faabric::BatchExecuteRequest> req)
{
    return broadcast();
}

int handleMpiCartCreate(tests::DistTestExecutor* exec,
                        int threadPoolIdx,
                        int msgIdx,
                        std::shared_ptr<faabric::BatchExecuteRequest> req)
{
    return cartCreate();
}

int handleMpiCartesian(tests::DistTestExecutor* exec,
                       int threadPoolIdx,
                       int msgIdx,
                       std::shared_ptr<faabric::BatchExecuteRequest> req)
{
    return cartesian();
}

int handleMpiChecks(tests::DistTestExecutor* exec,
                    int threadPoolIdx,
                    int msgIdx,
                    std::shared_ptr<faabric::BatchExecuteRequest> req)
{
    return checks();
}

int handleMpiGather(tests::DistTestExecutor* exec,
                    int threadPoolIdx,
                    int msgIdx,
                    std::shared_ptr<faabric::BatchExecuteRequest> req)
{
    return gather();
}

int handleMpiHelloWorld(tests::DistTestExecutor* exec,
                        int threadPoolIdx,
                        int msgIdx,
                        std::shared_ptr<faabric::BatchExecuteRequest> req)
{
    return helloWorld();
}

int handleMpiISendRecv(tests::DistTestExecutor* exec,
                       int threadPoolIdx,
                       int msgIdx,
                       std::shared_ptr<faabric::BatchExecuteRequest> req)
{
    return iSendRecv();
}

int handleMpiMigration(tests::DistTestExecutor* exec,
                       int threadPoolIdx,
                       int msgIdx,
                       std::shared_ptr<faabric::BatchExecuteRequest> req)
{
    auto* call = &req->mutable_messages()->at(msgIdx);

    return migration(std::stoi(call->inputdata()));
}

int handleMpiOrder(tests::DistTestExecutor* exec,
                   int threadPoolIdx,
                   int msgIdx,
                   std::shared_ptr<faabric::BatchExecuteRequest> req)
{
    return order();
}

int handleMpiReduce(tests::DistTestExecutor* exec,
                    int threadPoolIdx,
                    int msgIdx,
                    std::shared_ptr<faabric::BatchExecuteRequest> req)
{
    return reduce();
}

int handleMpiReduceMany(tests::DistTestExecutor* exec,
                        int threadPoolIdx,
                        int msgIdx,
                        std::shared_ptr<faabric::BatchExecuteRequest> req)
{
    return reduceMany();
}

int handleMpiScan(tests::DistTestExecutor* exec,
                  int threadPoolIdx,
                  int msgIdx,
                  std::shared_ptr<faabric::BatchExecuteRequest> req)
{
    return scan();
}

int handleMpiScatter(tests::DistTestExecutor* exec,
                     int threadPoolIdx,
                     int msgIdx,
                     std::shared_ptr<faabric::BatchExecuteRequest> req)
{
    return scatter();
}

int handleMpiSend(tests::DistTestExecutor* exec,
                  int threadPoolIdx,
                  int msgIdx,
                  std::shared_ptr<faabric::BatchExecuteRequest> req)
{
    return send();
}

int handleMpiSendMany(tests::DistTestExecutor* exec,
                      int threadPoolIdx,
                      int msgIdx,
                      std::shared_ptr<faabric::BatchExecuteRequest> req)
{
    return sendMany();
}

int handleMpiSendSyncAsync(tests::DistTestExecutor* exec,
                           int threadPoolIdx,
                           int msgIdx,
                           std::shared_ptr<faabric::BatchExecuteRequest> req)
{
    return sendSyncAsync();
}

int handleMpiSendRecv(tests::DistTestExecutor* exec,
                      int threadPoolIdx,
                      int msgIdx,
                      std::shared_ptr<faabric::BatchExecuteRequest> req)
{
    return sendRecv();
}

int handleMpiStatus(tests::DistTestExecutor* exec,
                    int threadPoolIdx,
                    int msgIdx,
                    std::shared_ptr<faabric::BatchExecuteRequest> req)
{
    return status();
}

int handleMpiTypeSize(tests::DistTestExecutor* exec,
                      int threadPoolIdx,
                      int msgIdx,
                      std::shared_ptr<faabric::BatchExecuteRequest> req)
{
    return typeSize();
}

void registerMpiTestFunctions()
{
    registerDistTestExecutorCallback(
      "mpi", "bench-allreduce", handleMpiBenchAllReduce);
    registerDistTestExecutorCallback("mpi", "allgather", handleMpiAllGather);
    registerDistTestExecutorCallback("mpi", "allreduce", handleMpiAllReduce);
    registerDistTestExecutorCallback("mpi", "alltoall", handleMpiAllToAll);
    registerDistTestExecutorCallback(
      "mpi", "alltoall-sleep", handleMpiAllToAllAndSleep);
    registerDistTestExecutorCallback("mpi", "barrier", handleMpiBarrier);
    registerDistTestExecutorCallback("mpi", "bcast", handleMpiBcast);
    registerDistTestExecutorCallback("mpi", "cart-create", handleMpiCartCreate);
    registerDistTestExecutorCallback("mpi", "cartesian", handleMpiCartesian);
    registerDistTestExecutorCallback("mpi", "checks", handleMpiChecks);
    registerDistTestExecutorCallback("mpi", "gather", handleMpiGather);
    registerDistTestExecutorCallback("mpi", "hello-world", handleMpiHelloWorld);
    registerDistTestExecutorCallback("mpi", "isendrecv", handleMpiISendRecv);
    registerDistTestExecutorCallback("mpi", "migration", handleMpiMigration);
    registerDistTestExecutorCallback("mpi", "order", handleMpiOrder);
    registerDistTestExecutorCallback("mpi", "reduce", handleMpiReduce);
    registerDistTestExecutorCallback("mpi", "reduce-many", handleMpiReduceMany);
    registerDistTestExecutorCallback("mpi", "scan", handleMpiScan);
    registerDistTestExecutorCallback("mpi", "scatter", handleMpiScatter);
    registerDistTestExecutorCallback("mpi", "send", handleMpiSend);
    registerDistTestExecutorCallback("mpi", "send-many", handleMpiSendMany);
    registerDistTestExecutorCallback(
      "mpi", "send-sync-async", handleMpiSendSyncAsync);
    registerDistTestExecutorCallback("mpi", "sendrecv", handleMpiSendRecv);
    registerDistTestExecutorCallback("mpi", "status", handleMpiStatus);
    registerDistTestExecutorCallback("mpi", "typesize", handleMpiTypeSize);
}
}
