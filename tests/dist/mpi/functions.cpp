#include <catch2/catch.hpp>

#include "DistTestExecutor.h"
#include "init.h"
#include "mpi/mpi_native.h"

#include <faabric/proto/faabric.pb.h>
#include <faabric/util/logging.h>

using namespace tests::mpi;

namespace tests {

faabric::Message* tests::mpi::executingCall;

/*
int handleMpiAllGather(tests::DistTestExecutor* exec,
                       int threadPoolIdx,
                       int msgIdx,
                       std::shared_ptr<faabric::BatchExecuteRequest> req)
{
    executingCall = &req->mutable_messages()->at(msgIdx);

    return allGather();
}

int handleMpiAllReduce(tests::DistTestExecutor* exec,
                       int threadPoolIdx,
                       int msgIdx,
                       std::shared_ptr<faabric::BatchExecuteRequest> req)
{
    executingCall = &req->mutable_messages()->at(msgIdx);

    return allReduce();
}

int handleMpiAllToAll(tests::DistTestExecutor* exec,
                      int threadPoolIdx,
                      int msgIdx,
                      std::shared_ptr<faabric::BatchExecuteRequest> req)
{
    executingCall = &req->mutable_messages()->at(msgIdx);

    return allToAll();
}

int handleMpiBcast(tests::DistTestExecutor* exec,
                   int threadPoolIdx,
                   int msgIdx,
                   std::shared_ptr<faabric::BatchExecuteRequest> req)
{
    executingCall = &req->mutable_messages()->at(msgIdx);

    return broadcast();
}

int handleMpiCartCreate(tests::DistTestExecutor* exec,
                        int threadPoolIdx,
                        int msgIdx,
                        std::shared_ptr<faabric::BatchExecuteRequest> req)
{
    executingCall = &req->mutable_messages()->at(msgIdx);

    return cartCreate();
}

int handleMpiCartesian(tests::DistTestExecutor* exec,
                       int threadPoolIdx,
                       int msgIdx,
                       std::shared_ptr<faabric::BatchExecuteRequest> req)
{
    executingCall = &req->mutable_messages()->at(msgIdx);

    return cartesian();
}

int handleMpiChecks(tests::DistTestExecutor* exec,
                    int threadPoolIdx,
                    int msgIdx,
                    std::shared_ptr<faabric::BatchExecuteRequest> req)
{
    executingCall = &req->mutable_messages()->at(msgIdx);

    return checks();
}

*/
int handleMpiGather(tests::DistTestExecutor* exec,
                    int threadPoolIdx,
                    int msgIdx,
                    std::shared_ptr<faabric::BatchExecuteRequest> req)
{
    executingCall = &req->mutable_messages()->at(msgIdx);

    return gather();
}

int handleMpiHelloWorld(tests::DistTestExecutor* exec,
                        int threadPoolIdx,
                        int msgIdx,
                        std::shared_ptr<faabric::BatchExecuteRequest> req)
{
    executingCall = &req->mutable_messages()->at(msgIdx);

    return helloWorld();
}

/*
int handleMpiISendRecv(tests::DistTestExecutor* exec,
                       int threadPoolIdx,
                       int msgIdx,
                       std::shared_ptr<faabric::BatchExecuteRequest> req)
{
    executingCall = &req->mutable_messages()->at(msgIdx);

    return iSendRecv();
}

int handleMpiOneSided(tests::DistTestExecutor* exec,
                      int threadPoolIdx,
                      int msgIdx,
                      std::shared_ptr<faabric::BatchExecuteRequest> req)
{
    executingCall = &req->mutable_messages()->at(msgIdx);

    return oneSided();
}

int handleMpiOrder(tests::DistTestExecutor* exec,
                   int threadPoolIdx,
                   int msgIdx,
                   std::shared_ptr<faabric::BatchExecuteRequest> req)
{
    executingCall = &req->mutable_messages()->at(msgIdx);

    return order();
}

int handleMpiProbe(tests::DistTestExecutor* exec,
                   int threadPoolIdx,
                   int msgIdx,
                   std::shared_ptr<faabric::BatchExecuteRequest> req)
{
    executingCall = &req->mutable_messages()->at(msgIdx);

    return probe();
}

int handleMpiReduce(tests::DistTestExecutor* exec,
                    int threadPoolIdx,
                    int msgIdx,
                    std::shared_ptr<faabric::BatchExecuteRequest> req)
{
    executingCall = &req->mutable_messages()->at(msgIdx);

    return reduce();
}

int handleMpiScan(tests::DistTestExecutor* exec,
                  int threadPoolIdx,
                  int msgIdx,
                  std::shared_ptr<faabric::BatchExecuteRequest> req)
{
    executingCall = &req->mutable_messages()->at(msgIdx);

    return scan();
}

int handleMpiScatter(tests::DistTestExecutor* exec,
                     int threadPoolIdx,
                     int msgIdx,
                     std::shared_ptr<faabric::BatchExecuteRequest> req)
{
    executingCall = &req->mutable_messages()->at(msgIdx);

    return scatter();
}

int handleMpiSend(tests::DistTestExecutor* exec,
                  int threadPoolIdx,
                  int msgIdx,
                  std::shared_ptr<faabric::BatchExecuteRequest> req)
{
    executingCall = &req->mutable_messages()->at(msgIdx);

    return send();
}

int handleMpiSendRecv(tests::DistTestExecutor* exec,
                      int threadPoolIdx,
                      int msgIdx,
                      std::shared_ptr<faabric::BatchExecuteRequest> req)
{
    executingCall = &req->mutable_messages()->at(msgIdx);

    return sendRecv();
}

int handleMpiStatus(tests::DistTestExecutor* exec,
                    int threadPoolIdx,
                    int msgIdx,
                    std::shared_ptr<faabric::BatchExecuteRequest> req)
{
    executingCall = &req->mutable_messages()->at(msgIdx);

    return status();
}

int handleMpiTypeSize(tests::DistTestExecutor* exec,
                      int threadPoolIdx,
                      int msgIdx,
                      std::shared_ptr<faabric::BatchExecuteRequest> req)
{
    executingCall = &req->mutable_messages()->at(msgIdx);

    return typeSize();
}

int handleMpiWinCreate(tests::DistTestExecutor* exec,
                       int threadPoolIdx,
                       int msgIdx,
                       std::shared_ptr<faabric::BatchExecuteRequest> req)
{
    executingCall = &req->mutable_messages()->at(msgIdx);

    return winCreate();
}
*/

void registerMpiTestFunctions()
{
    /*
    registerDistTestExecutorCallback("mpi", "allgather", handleMpiAllGather);
    registerDistTestExecutorCallback("mpi", "allreduce", handleMpiAllReduce);
    registerDistTestExecutorCallback("mpi", "alltoall", handleMpiAllToAll);
    registerDistTestExecutorCallback("mpi", "bcast", handleMpiBcast);
    registerDistTestExecutorCallback("mpi", "cart-create", handleMpiCartCreate);
    registerDistTestExecutorCallback("mpi", "cartesian", handleMpiCartesian);
    registerDistTestExecutorCallback("mpi", "checks", handleMpiChecks);
    */
    registerDistTestExecutorCallback("mpi", "gather", handleMpiGather);
    registerDistTestExecutorCallback("mpi", "hello-world", handleMpiHelloWorld);
    /*
    registerDistTestExecutorCallback("mpi", "isendrecv", handleMpiISendRecv);
    registerDistTestExecutorCallback("mpi", "onesided", handleMpiOneSided);
    registerDistTestExecutorCallback("mpi", "order", handleMpiOrder);
    registerDistTestExecutorCallback("mpi", "probe", handleMpiProbe);
    registerDistTestExecutorCallback("mpi", "reduce", handleMpiReduce);
    registerDistTestExecutorCallback("mpi", "scan", handleMpiScan);
    registerDistTestExecutorCallback("mpi", "scatter", handleMpiScatter);
    registerDistTestExecutorCallback("mpi", "send", handleMpiSend);
    registerDistTestExecutorCallback("mpi", "sendrecv", handleMpiSendRecv);
    registerDistTestExecutorCallback("mpi", "status", handleMpiStatus);
    registerDistTestExecutorCallback("mpi", "typesize", handleMpiTypeSize);
    registerDistTestExecutorCallback("mpi", "win-create", handleMpiWinCreate);
    */
}
}
