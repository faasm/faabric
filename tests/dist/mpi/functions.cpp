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

    int returnValue = allGather();

    return returnValue;
}

int handleMpiAllReduce(faabric::scheduler::Executor* exec,
                       int threadPoolIdx,
                       int msgIdx,
                       std::shared_ptr<faabric::BatchExecuteRequest> req)
{
    executingCall = &req->mutable_messages()->at(msgIdx);

    int returnValue = allReduce();

    return returnValue;
}

int handleMpiAllToAll(faabric::scheduler::Executor* exec,
                      int threadPoolIdx,
                      int msgIdx,
                      std::shared_ptr<faabric::BatchExecuteRequest> req)
{
    executingCall = &req->mutable_messages()->at(msgIdx);

    int returnValue = allToAll();

    return returnValue;
}

int handleMpiBcast(faabric::scheduler::Executor* exec,
                   int threadPoolIdx,
                   int msgIdx,
                   std::shared_ptr<faabric::BatchExecuteRequest> req)
{
    executingCall = &req->mutable_messages()->at(msgIdx);

    int returnValue = broadcast();

    return returnValue;
}

int handleMpiCartCreate(faabric::scheduler::Executor* exec,
                        int threadPoolIdx,
                        int msgIdx,
                        std::shared_ptr<faabric::BatchExecuteRequest> req)
{
    executingCall = &req->mutable_messages()->at(msgIdx);

    int returnValue = cartCreate();

    return returnValue;
}

int handleMpiCartesian(faabric::scheduler::Executor* exec,
                       int threadPoolIdx,
                       int msgIdx,
                       std::shared_ptr<faabric::BatchExecuteRequest> req)
{
    executingCall = &req->mutable_messages()->at(msgIdx);

    int returnValue = cartesian();

    return returnValue;
}

int handleMpiChecks(faabric::scheduler::Executor* exec,
                    int threadPoolIdx,
                    int msgIdx,
                    std::shared_ptr<faabric::BatchExecuteRequest> req)
{
    executingCall = &req->mutable_messages()->at(msgIdx);

    int returnValue = checks();

    return returnValue;
}

int handleMpiGather(faabric::scheduler::Executor* exec,
                    int threadPoolIdx,
                    int msgIdx,
                    std::shared_ptr<faabric::BatchExecuteRequest> req)
{
    executingCall = &req->mutable_messages()->at(msgIdx);

    int returnValue = gather();

    return returnValue;
}

int handleMpiHelloWorld(faabric::scheduler::Executor* exec,
                        int threadPoolIdx,
                        int msgIdx,
                        std::shared_ptr<faabric::BatchExecuteRequest> req)
{
    executingCall = &req->mutable_messages()->at(msgIdx);

    int returnValue = helloWorld();

    return returnValue;
}

int handleMpiISendRecv(faabric::scheduler::Executor* exec,
                       int threadPoolIdx,
                       int msgIdx,
                       std::shared_ptr<faabric::BatchExecuteRequest> req)
{
    executingCall = &req->mutable_messages()->at(msgIdx);

    int returnValue = iSendRecv();

    return returnValue;
}

int handleMpiOneSided(faabric::scheduler::Executor* exec,
                      int threadPoolIdx,
                      int msgIdx,
                      std::shared_ptr<faabric::BatchExecuteRequest> req)
{
    executingCall = &req->mutable_messages()->at(msgIdx);

    int returnValue = oneSided();

    return returnValue;
}

int handleMpiOrder(faabric::scheduler::Executor* exec,
                   int threadPoolIdx,
                   int msgIdx,
                   std::shared_ptr<faabric::BatchExecuteRequest> req)
{
    executingCall = &req->mutable_messages()->at(msgIdx);

    int returnValue = order();

    return returnValue;
}

int handleMpiProbe(faabric::scheduler::Executor* exec,
                   int threadPoolIdx,
                   int msgIdx,
                   std::shared_ptr<faabric::BatchExecuteRequest> req)
{
    executingCall = &req->mutable_messages()->at(msgIdx);

    int returnValue = probe();

    return returnValue;
}

int handleMpiReduce(faabric::scheduler::Executor* exec,
                    int threadPoolIdx,
                    int msgIdx,
                    std::shared_ptr<faabric::BatchExecuteRequest> req)
{
    executingCall = &req->mutable_messages()->at(msgIdx);

    int returnValue = reduce();

    return returnValue;
}

int handleMpiScan(faabric::scheduler::Executor* exec,
                  int threadPoolIdx,
                  int msgIdx,
                  std::shared_ptr<faabric::BatchExecuteRequest> req)
{
    executingCall = &req->mutable_messages()->at(msgIdx);

    int returnValue = scan();

    return returnValue;
}

int handleMpiScatter(faabric::scheduler::Executor* exec,
                     int threadPoolIdx,
                     int msgIdx,
                     std::shared_ptr<faabric::BatchExecuteRequest> req)
{
    executingCall = &req->mutable_messages()->at(msgIdx);

    int returnValue = scatter();

    return returnValue;
}

int handleMpiSend(faabric::scheduler::Executor* exec,
                  int threadPoolIdx,
                  int msgIdx,
                  std::shared_ptr<faabric::BatchExecuteRequest> req)
{
    executingCall = &req->mutable_messages()->at(msgIdx);

    int returnValue = send();

    return returnValue;
}

int handleMpiSendRecv(faabric::scheduler::Executor* exec,
                      int threadPoolIdx,
                      int msgIdx,
                      std::shared_ptr<faabric::BatchExecuteRequest> req)
{
    executingCall = &req->mutable_messages()->at(msgIdx);

    int returnValue = sendRecv();

    return returnValue;
}

int handleMpiStatus(faabric::scheduler::Executor* exec,
                    int threadPoolIdx,
                    int msgIdx,
                    std::shared_ptr<faabric::BatchExecuteRequest> req)
{
    executingCall = &req->mutable_messages()->at(msgIdx);

    int returnValue = status();

    return returnValue;
}

int handleMpiTypeSize(faabric::scheduler::Executor* exec,
                      int threadPoolIdx,
                      int msgIdx,
                      std::shared_ptr<faabric::BatchExecuteRequest> req)
{
    executingCall = &req->mutable_messages()->at(msgIdx);

    int returnValue = typeSize();

    return returnValue;
}

int handleMpiWinCreate(faabric::scheduler::Executor* exec,
                       int threadPoolIdx,
                       int msgIdx,
                       std::shared_ptr<faabric::BatchExecuteRequest> req)
{
    executingCall = &req->mutable_messages()->at(msgIdx);

    int returnValue = winCreate();

    return returnValue;
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
