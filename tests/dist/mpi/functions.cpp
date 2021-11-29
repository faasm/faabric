#include <catch2/catch.hpp>

#include "DistTestExecutor.h"
#include "init.h"
#include "mpi/mpi_native.h"

#include <faabric/proto/faabric.pb.h>
#include <faabric/util/logging.h>

namespace tests {

faabric::Message* executingCall;

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

void registerMpiTestFunctions()
{
    registerDistTestExecutorCallback("mpi", "allgather", handleMpiAllGather);
    registerDistTestExecutorCallback("mpi", "allreduce", handleMpiAllReduce);
    registerDistTestExecutorCallback("mpi", "alltoall", handleMpiAllToAll);
    registerDistTestExecutorCallback("mpi", "bcast", handleMpiBcast);
    registerDistTestExecutorCallback("mpi", "cart-create", handleMpiCartCreate);
    registerDistTestExecutorCallback("mpi", "cartesian", handleMpiCartesian);
    registerDistTestExecutorCallback("mpi", "checks", handleMpiChecks);
}
}
