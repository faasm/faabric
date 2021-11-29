#include <catch2/catch.hpp>

#include "DistTestExecutor.h"
#include "init.h"
#include "mpi/mpi_native.h"

#include <faabric/proto/faabric.pb.h>
#include <faabric/util/logging.h>

using namespace tests::mpi;

namespace tests {

faabric::Message* tests::mpi::executingCall;

int handleMpiAllGather(faabric::scheduler::Executor* exec,
                       int threadPoolIdx,
                       int msgIdx,
                       std::shared_ptr<faabric::BatchExecuteRequest> req)
{
    executingCall = &req->mutable_messages()->at(msgIdx);

    return allGather();
}

int handleMpiAllReduce(faabric::scheduler::Executor* exec,
                       int threadPoolIdx,
                       int msgIdx,
                       std::shared_ptr<faabric::BatchExecuteRequest> req)
{
    executingCall = &req->mutable_messages()->at(msgIdx);

    return allReduce();
}

int handleMpiAllToAll(faabric::scheduler::Executor* exec,
                      int threadPoolIdx,
                      int msgIdx,
                      std::shared_ptr<faabric::BatchExecuteRequest> req)
{
    executingCall = &req->mutable_messages()->at(msgIdx);

    return allToAll();
}

int handleMpiBcast(faabric::scheduler::Executor* exec,
                   int threadPoolIdx,
                   int msgIdx,
                   std::shared_ptr<faabric::BatchExecuteRequest> req)
{
    executingCall = &req->mutable_messages()->at(msgIdx);

    return broadcast();
}

int handleMpiCartCreate(faabric::scheduler::Executor* exec,
                        int threadPoolIdx,
                        int msgIdx,
                        std::shared_ptr<faabric::BatchExecuteRequest> req)
{
    executingCall = &req->mutable_messages()->at(msgIdx);

    return cartCreate();
}

int handleMpiCartesian(faabric::scheduler::Executor* exec,
                       int threadPoolIdx,
                       int msgIdx,
                       std::shared_ptr<faabric::BatchExecuteRequest> req)
{
    executingCall = &req->mutable_messages()->at(msgIdx);

    return cartesian();
}

int handleMpiChecks(faabric::scheduler::Executor* exec,
                    int threadPoolIdx,
                    int msgIdx,
                    std::shared_ptr<faabric::BatchExecuteRequest> req)
{
    executingCall = &req->mutable_messages()->at(msgIdx);

    return checks();
}

int handleMpiGather(faabric::scheduler::Executor* exec,
                    int threadPoolIdx,
                    int msgIdx,
                    std::shared_ptr<faabric::BatchExecuteRequest> req)
{
    executingCall = &req->mutable_messages()->at(msgIdx);

    return gather();
}

int handleMpiHelloWorld(faabric::scheduler::Executor* exec,
                        int threadPoolIdx,
                        int msgIdx,
                        std::shared_ptr<faabric::BatchExecuteRequest> req)
{
    executingCall = &req->mutable_messages()->at(msgIdx);

    return helloWorld();
}

int handleMpiISendRecv(faabric::scheduler::Executor* exec,
                       int threadPoolIdx,
                       int msgIdx,
                       std::shared_ptr<faabric::BatchExecuteRequest> req)
{
    executingCall = &req->mutable_messages()->at(msgIdx);

    return iSendRecv();
}

int handleMpiOneSided(faabric::scheduler::Executor* exec,
                      int threadPoolIdx,
                      int msgIdx,
                      std::shared_ptr<faabric::BatchExecuteRequest> req)
{
    executingCall = &req->mutable_messages()->at(msgIdx);

    return oneSided();
}

int handleMpiOrder(faabric::scheduler::Executor* exec,
                   int threadPoolIdx,
                   int msgIdx,
                   std::shared_ptr<faabric::BatchExecuteRequest> req)
{
    executingCall = &req->mutable_messages()->at(msgIdx);

    return order();
}

int handleMpiProbe(faabric::scheduler::Executor* exec,
                   int threadPoolIdx,
                   int msgIdx,
                   std::shared_ptr<faabric::BatchExecuteRequest> req)
{
    executingCall = &req->mutable_messages()->at(msgIdx);

    return probe();
}

int handleMpiReduce(faabric::scheduler::Executor* exec,
                    int threadPoolIdx,
                    int msgIdx,
                    std::shared_ptr<faabric::BatchExecuteRequest> req)
{
    executingCall = &req->mutable_messages()->at(msgIdx);

    return reduce();
}

int handleMpiScan(faabric::scheduler::Executor* exec,
                  int threadPoolIdx,
                  int msgIdx,
                  std::shared_ptr<faabric::BatchExecuteRequest> req)
{
    executingCall = &req->mutable_messages()->at(msgIdx);

    return scan();
}

int handleMpiScatter(faabric::scheduler::Executor* exec,
                     int threadPoolIdx,
                     int msgIdx,
                     std::shared_ptr<faabric::BatchExecuteRequest> req)
{
    executingCall = &req->mutable_messages()->at(msgIdx);

    return scatter();
}

int handleMpiSend(faabric::scheduler::Executor* exec,
                  int threadPoolIdx,
                  int msgIdx,
                  std::shared_ptr<faabric::BatchExecuteRequest> req)
{
    executingCall = &req->mutable_messages()->at(msgIdx);

    return send();
}

int handleMpiSendRecv(faabric::scheduler::Executor* exec,
                      int threadPoolIdx,
                      int msgIdx,
                      std::shared_ptr<faabric::BatchExecuteRequest> req)
{
    executingCall = &req->mutable_messages()->at(msgIdx);

    return sendRecv();
}

int handleMpiStatus(faabric::scheduler::Executor* exec,
                    int threadPoolIdx,
                    int msgIdx,
                    std::shared_ptr<faabric::BatchExecuteRequest> req)
{
    executingCall = &req->mutable_messages()->at(msgIdx);

    return status();
}

int handleMpiTypeSize(faabric::scheduler::Executor* exec,
                      int threadPoolIdx,
                      int msgIdx,
                      std::shared_ptr<faabric::BatchExecuteRequest> req)
{
    executingCall = &req->mutable_messages()->at(msgIdx);

    return typeSize();
}

int handleMpiWinCreate(faabric::scheduler::Executor* exec,
                       int threadPoolIdx,
                       int msgIdx,
                       std::shared_ptr<faabric::BatchExecuteRequest> req)
{
    executingCall = &req->mutable_messages()->at(msgIdx);

    return winCreate();
}

void registerMpiTestFunctions()
{
    registerDistTestExecutorCallback("mpi", "allgather", handleMpiAllGather);
    registerDistTestExecutorCallback("mpi", "allreduce", handleMpiAllReduce);
    registerDistTestExecutorCallback("mpi", "alltoall", handleMpiAllToAll);
    registerDistTestExecutorCallback("mpi", "bcast", handleMpiBcast);
    registerDistTestExecutorCallback("mpi", "cart-create", handleMpiCartCreate);
    registerDistTestExecutorCallback("mpi", "cartesian", handleMpiCartesian);
    registerDistTestExecutorCallback("mpi", "checks", handleMpiChecks);
    registerDistTestExecutorCallback("mpi", "gather", handleMpiGather);
    registerDistTestExecutorCallback("mpi", "hello-world", handleMpiHelloWorld);
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
}
}
