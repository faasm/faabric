#include <faabric/mpi-native/MpiExecutor.h>
#include <faabric/mpi/mpi.h>

#include <faabric/scheduler/MpiContext.h>
#include <faabric/scheduler/MpiWorld.h>

#include <faabric/util/logging.h>

using namespace faabric::executor;

static faabric::scheduler::MpiContext executingContext;

faabric::Message* getExecutingCall()
{
    return faabric::executor::executingCall;
}

faabric::scheduler::MpiWorld& getExecutingWorld()
{
    int worldId = executingContext.getWorldId();
    faabric::scheduler::MpiWorldRegistry& reg =
      faabric::scheduler::getMpiWorldRegistry();
    return reg.getOrInitialiseWorld(*getExecutingCall(), worldId);
}

static void notImplemented(const std::string& funcName)
{
    auto logger = faabric::util::getLogger();
    logger->debug("S - {}", funcName);

    throw std::runtime_error(funcName + " not implemented.");
}

/*
 * Template function to call MPI implementations in MpiWorld
 */
template<class T, typename... Args>
void callMpiFunc(const std::string& funcName, int* ret, T f, Args... args)
{
    auto logger = faabric::util::getLogger();
    logger->debug("S - {}", funcName);

    faabric::scheduler::MpiWorld& world = getExecutingWorld();
    *ret = (world.*f)(args...);
}

int MPI_Init(int* argc, char*** argv)
{
    auto logger = faabric::util::getLogger();

    faabric::Message* call = getExecutingCall();

    if (call->mpirank() <= 0) {
        logger->debug("S - MPI_Init (create)");
        executingContext.createWorld(*call);
    } else {
        logger->debug("S - MPI_Init (join)");
        executingContext.joinWorld(*call);
    }

    int thisRank = executingContext.getRank();
    faabric::scheduler::MpiWorld& world = getExecutingWorld();
    world.barrier(thisRank);

    return MPI_SUCCESS;
}

int MPI_Comm_rank(MPI_Comm comm, int* rank)
{
    auto logger = faabric::util::getLogger();
    logger->debug("S - MPI_Comm_rank");

    *rank = executingContext.getRank();

    return MPI_SUCCESS;
}

int MPI_Comm_size(MPI_Comm comm, int* size)
{
    /*
    auto logger = faabric::util::getLogger();
    logger->debug("S - MPI_Comm_size");

    faabric::scheduler::MpiWorld& world = getExecutingWorld();
    *size = world.getSize();
    */
    auto fptr = &faabric::scheduler::MpiWorld::getSize;
    callMpiFunc("MPI_Comm_size", size, fptr);

    return MPI_SUCCESS;
}

int MPI_Finalize()
{
    auto logger = faabric::util::getLogger();
    logger->debug("S - MPI_Finalize");

    return MPI_SUCCESS;
}

int MPI_Get_version(int* version, int* subversion)
{
    notImplemented("MPI_Get_version");

    return MPI_SUCCESS;
}

int MPI_Send(const void* buf,
             int count,
             MPI_Datatype datatype,
             int dest,
             int tag,
             MPI_Comm comm)
{
    auto logger = faabric::util::getLogger();
    logger->debug("S - MPI_Send {} -> {}", executingContext.getRank(), dest);

    faabric::scheduler::MpiWorld& world = getExecutingWorld();
    world.send(
      executingContext.getRank(), dest, (uint8_t*)buf, datatype, count);

    return MPI_SUCCESS;
}

int MPI_Rsend(const void* buf,
              int count,
              MPI_Datatype datatype,
              int dest,
              int tag,
              MPI_Comm comm)
{
    notImplemented("MPI_Rsend");

    return MPI_SUCCESS;
}

int MPI_Recv(void* buf,
             int count,
             MPI_Datatype datatype,
             int source,
             int tag,
             MPI_Comm comm,
             MPI_Status* status)
{
    auto logger = faabric::util::getLogger();
    logger->debug("S - MPI_Recv {} <- {}", executingContext.getRank(), source);

    faabric::scheduler::MpiWorld& world = getExecutingWorld();
    world.recv(source,
               executingContext.getRank(),
               (uint8_t*)buf,
               datatype,
               count,
               status);

    return MPI_SUCCESS;
}

int MPI_Sendrecv(const void* sendbuf,
                 int sendcount,
                 MPI_Datatype sendtype,
                 int dest,
                 int sendtag,
                 void* recvbuf,
                 int recvcount,
                 MPI_Datatype recvtype,
                 int source,
                 int recvtag,
                 MPI_Comm comm,
                 MPI_Status* status)
{
    auto logger = faabric::util::getLogger();
    logger->debug("S - MPI_Sendrecv {} -> {} and {} <- {}",
                  executingContext.getRank(),
                  dest,
                  executingContext.getRank(),
                  source);

    faabric::scheduler::MpiWorld& world = getExecutingWorld();
    world.sendRecv((uint8_t*)sendbuf,
                   sendcount,
                   sendtype,
                   dest,
                   (uint8_t*)recvbuf,
                   recvcount,
                   recvtype,
                   source,
                   executingContext.getRank(),
                   status);

    return MPI_SUCCESS;
}

int MPI_Abort(MPI_Comm comm, int errorcode)
{
    auto logger = faabric::util::getLogger();
    logger->debug("S - MPI_Abort");

    return MPI_SUCCESS;
}

int MPI_Get_count(const MPI_Status* status, MPI_Datatype datatype, int* count)
{
    auto logger = faabric::util::getLogger();
    logger->debug("S - MPI_Get_count");

    // Implement
    if (status->bytesSize % datatype->size != 0) {
        logger->error("Incomplete message (bytes {}, datatype size {})", status->bytesSize, datatype->size);
        return 1;
    }

    *count = status->bytesSize / datatype->size;

    return MPI_SUCCESS;
}

int MPI_Probe(int source, int tag, MPI_Comm comm, MPI_Status* status)
{
    auto logger = faabric::util::getLogger();
    logger->debug("S - MPI_Probe");

    return MPI_SUCCESS;
}
