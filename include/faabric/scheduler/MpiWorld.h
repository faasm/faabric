#pragma once

#include <faabric/mpi/mpi.h>

#include <faabric/proto/faabric.pb.h>
#include <faabric/scheduler/FunctionCallClient.h>
#include <faabric/scheduler/InMemoryMessageQueue.h>
#include <faabric/scheduler/MpiThreadPool.h>
#include <faabric/state/StateKeyValue.h>

#include <atomic>
#include <thread>

namespace faabric::scheduler {
typedef faabric::util::Queue<std::shared_ptr<faabric::MPIMessage>>
  InMemoryMpiQueue;

std::string getWorldStateKey(int worldId);

class MpiWorld
{
  public:
    MpiWorld();

    void create(const faabric::Message& call, int newId, int newSize);

    void initialiseFromMsg(const faabric::Message& msg,
                           bool forceLocal = false);

    std::string getHostForRank(int rank);

    void setAllRankHosts(const faabric::MpiHostsToRanksMessage& msg);

    std::string getUser();

    std::string getFunction();

    int getId();

    int getSize();

    void destroy();

    void shutdownThreadPool();

    void enqueueMessage(faabric::MPIMessage& msg);

    void getCartesianRank(int rank,
                          int maxDims,
                          const int* dims,
                          int* periods,
                          int* coords);

    void getRankFromCoords(int* rank, int* coords);

    void shiftCartesianCoords(int rank,
                              int direction,
                              int disp,
                              int* source,
                              int* destination);

    void send(int sendRank,
              int recvRank,
              const uint8_t* buffer,
              faabric_datatype_t* dataType,
              int count,
              faabric::MPIMessage::MPIMessageType messageType =
                faabric::MPIMessage::NORMAL);

    int isend(int sendRank,
              int recvRank,
              const uint8_t* buffer,
              faabric_datatype_t* dataType,
              int count,
              faabric::MPIMessage::MPIMessageType messageType =
                faabric::MPIMessage::NORMAL);

    void broadcast(int sendRank,
                   const uint8_t* buffer,
                   faabric_datatype_t* dataType,
                   int count,
                   faabric::MPIMessage::MPIMessageType messageType =
                     faabric::MPIMessage::NORMAL);

    void recv(int sendRank,
              int recvRank,
              uint8_t* buffer,
              faabric_datatype_t* dataType,
              int count,
              MPI_Status* status,
              faabric::MPIMessage::MPIMessageType messageType =
                faabric::MPIMessage::NORMAL);

    int irecv(int sendRank,
              int recvRank,
              uint8_t* buffer,
              faabric_datatype_t* dataType,
              int count,
              faabric::MPIMessage::MPIMessageType messageType =
                faabric::MPIMessage::NORMAL);

    void awaitAsyncRequest(int requestId);

    void sendRecv(uint8_t* sendBuffer,
                  int sendcount,
                  faabric_datatype_t* sendDataType,
                  int sendRank,
                  uint8_t* recvBuffer,
                  int recvCount,
                  faabric_datatype_t* recvDataType,
                  int recvRank,
                  int myRank,
                  MPI_Status* status);

    void scatter(int sendRank,
                 int recvRank,
                 const uint8_t* sendBuffer,
                 faabric_datatype_t* sendType,
                 int sendCount,
                 uint8_t* recvBuffer,
                 faabric_datatype_t* recvType,
                 int recvCount);

    void gather(int sendRank,
                int recvRank,
                const uint8_t* sendBuffer,
                faabric_datatype_t* sendType,
                int sendCount,
                uint8_t* recvBuffer,
                faabric_datatype_t* recvType,
                int recvCount);

    void allGather(int rank,
                   const uint8_t* sendBuffer,
                   faabric_datatype_t* sendType,
                   int sendCount,
                   uint8_t* recvBuffer,
                   faabric_datatype_t* recvType,
                   int recvCount);

    void reduce(int sendRank,
                int recvRank,
                uint8_t* sendBuffer,
                uint8_t* recvBuffer,
                faabric_datatype_t* datatype,
                int count,
                faabric_op_t* operation);

    void allReduce(int rank,
                   uint8_t* sendBuffer,
                   uint8_t* recvBuffer,
                   faabric_datatype_t* datatype,
                   int count,
                   faabric_op_t* operation);

    void op_reduce(faabric_op_t* operation,
                   faabric_datatype_t* datatype,
                   int count,
                   uint8_t* inBuffer,
                   uint8_t* resultBuffer);

    void scan(int rank,
              uint8_t* sendBuffer,
              uint8_t* recvBuffer,
              faabric_datatype_t* datatype,
              int count,
              faabric_op_t* operation);

    void allToAll(int rank,
                  uint8_t* sendBuffer,
                  faabric_datatype_t* sendType,
                  int sendCount,
                  uint8_t* recvBuffer,
                  faabric_datatype_t* recvType,
                  int recvCount);

    void probe(int sendRank, int recvRank, MPI_Status* status);

    void barrier(int thisRank);

    void rmaGet(int sendRank,
                faabric_datatype_t* sendType,
                int sendCount,
                uint8_t* recvBuffer,
                faabric_datatype_t* recvType,
                int recvCount);

    void rmaPut(int sendRank,
                uint8_t* sendBuffer,
                faabric_datatype_t* sendType,
                int sendCount,
                int recvRank,
                faabric_datatype_t* recvType,
                int recvCount);

    std::shared_ptr<InMemoryMpiQueue> getLocalQueue(int sendRank, int recvRank);

    long getLocalQueueSize(int sendRank, int recvRank);

    void overrideHost(const std::string& newHost);

    void createWindow(const int winRank, const int winSize, uint8_t* windowPtr);

    void synchronizeRmaWrite(const faabric::MPIMessage& msg, bool isRemote);

    double getWTime();

  private:
    int id;
    int size;
    std::string thisHost;
    faabric::util::TimePoint creationTime;

    const std::shared_ptr<spdlog::logger> logger;

    std::shared_mutex worldMutex;
    std::atomic_flag isDestroyed = false;

    std::string user;
    std::string function;

    std::shared_ptr<state::StateKeyValue> stateKV;
    std::unordered_map<int, std::string> rankHostMap;

    std::unordered_map<std::string, uint8_t*> windowPointerMap;

    std::unordered_map<std::string, std::shared_ptr<InMemoryMpiQueue>>
      localQueueMap;

    std::shared_ptr<faabric::scheduler::MpiAsyncThreadPool> threadPool;
    int getMpiThreadPoolSize();

    std::vector<int> cartProcsPerDim;

    faabric::scheduler::FunctionCallClient& getFunctionCallClient(
      const std::string& otherHost);

    void closeThreadLocalClients();
};
}
