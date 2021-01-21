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

template<class T, typename... Args>
void callMpiFunc(const std::string& funcName, T f, Args... args)
{
    auto logger = faabric::util::getLogger();
    logger->debug("S - {}", funcName);

    faabric::scheduler::MpiWorld& world = getExecutingWorld();
    (world.*f)(args...);
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
    auto fptr = &faabric::scheduler::MpiWorld::getSizePtr;
    callMpiFunc("MPI_Comm_size", fptr, size);

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
    auto fptr = &faabric::scheduler::MpiWorld::send;
    callMpiFunc(
        fmt::format("MPI_Send {} -> {}", executingContext.getRank(), dest),
        fptr,
        executingContext.getRank(),
        dest, 
        (uint8_t*)buf, 
        datatype,
        count,
        faabric::MPIMessage::NORMAL);

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
    auto fptr = &faabric::scheduler::MpiWorld::recv;
    callMpiFunc(
        fmt::format("MPI_Recv {} <- {}", executingContext.getRank(), source),
        fptr,
        source,
        executingContext.getRank(),
        (uint8_t*)buf, 
        datatype,
        count,
        status,
        faabric::MPIMessage::NORMAL);

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
    auto fptr = &faabric::scheduler::MpiWorld::sendRecv;
    callMpiFunc(
        fmt::format("MPI_Sendrecv {} -> {} and {} <- {}", executingContext.getRank(), dest,
            executingContext.getRank(), source),
        fptr,
        (uint8_t*)sendbuf,
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

    if (status->bytesSize % datatype->size != 0) {
        logger->error("Incomplete message (bytes {}, datatype size {})", status->bytesSize, datatype->size);
        return 1;
    }

    *count = status->bytesSize / datatype->size;

    return MPI_SUCCESS;
}

int MPI_Probe(int source, int tag, MPI_Comm comm, MPI_Status* status)
{
    auto fptr = &faabric::scheduler::MpiWorld::probe;
    callMpiFunc("MPI_Probe", fptr, source, executingContext.getRank(), status);

    return MPI_SUCCESS;
}

int MPI_Barrier(MPI_Comm comm)
{
    auto fptr = &faabric::scheduler::MpiWorld::barrier;
    callMpiFunc("MPI_Barrier", fptr, executingContext.getRank());

    return MPI_SUCCESS;
}

int MPI_Bcast(void* buffer,
              int count,
              MPI_Datatype datatype,
              int root,
              MPI_Comm comm)
{
    int rank = executingContext.getRank();
    if (rank == root) {
        auto fptr = &faabric::scheduler::MpiWorld::broadcast;
        callMpiFunc(fmt::format("MPI_Bcast {} -> all", rank),
                    fptr,
                    rank, 
                    (uint8_t*) buffer,
                    datatype,
                    count,
                    faabric::MPIMessage::NORMAL);
    } else {
        auto fptr = &faabric::scheduler::MpiWorld::recv;
        callMpiFunc(fmt::format("MPI_Bcast {} <- {}", rank, root),
                    fptr,
                    root,
                    rank,
                    (uint8_t*) buffer,
                    datatype,
                    count,
                    nullptr,
                    faabric::MPIMessage::NORMAL);
    }
    return MPI_SUCCESS;
}

int MPI_Scatter(const void* sendbuf,
                int sendcount,
                MPI_Datatype sendtype,
                void* recvbuf,
                int recvcount,
                MPI_Datatype recvtype,
                int root,
                MPI_Comm comm)
{
    auto fptr = &faabric::scheduler::MpiWorld::scatter;
    callMpiFunc(fmt::format("MPI_Scatter {} -> {}", root, executingContext.getRank()),
                fptr,
                root,
                executingContext.getRank(),
                (uint8_t*) sendbuf,
                sendtype,
                sendcount,
                (uint8_t*) recvbuf,
                recvtype,
                recvcount);

    return MPI_SUCCESS;
}

int MPI_Gather(const void* sendbuf,
               int sendcount,
               MPI_Datatype sendtype,
               void* recvbuf,
               int recvcount,
               MPI_Datatype recvtype,
               int root,
               MPI_Comm comm)
{
    auto fptr = &faabric::scheduler::MpiWorld::gather;
    callMpiFunc("MPI_Gather",
                fptr,
                executingContext.getRank(),
                root,
                (uint8_t*) sendbuf,
                sendtype,
                sendcount,
                (uint8_t*) recvbuf,
                recvtype,
                recvcount);

    return MPI_SUCCESS;
}

int MPI_Gatherv(const void* sendbuf,
                int sendcount,
                MPI_Datatype sendtype,
                void* recvbuf,
                const int* recvcounts,
                const int* displs,
                MPI_Datatype recvtype,
                int root,
                MPI_Comm comm);

int MPI_Allgather(const void* sendbuf,
                  int sendcount,
                  MPI_Datatype sendtype,
                  void* recvbuf,
                  int recvcount,
                  MPI_Datatype recvtype,
                  MPI_Comm comm);

int MPI_Allgatherv(const void* sendbuf,
                   int sendcount,
                   MPI_Datatype sendtype,
                   void* recvbuf,
                   const int* recvcounts,
                   const int* displs,
                   MPI_Datatype recvtype,
                   MPI_Comm comm);

int MPI_Reduce(const void* sendbuf,
               void* recvbuf,
               int count,
               MPI_Datatype datatype,
               MPI_Op op,
               int root,
               MPI_Comm comm);

int MPI_Reduce_scatter(const void* sendbuf,
                       void* recvbuf,
                       const int* recvcounts,
                       MPI_Datatype datatype,
                       MPI_Op op,
                       MPI_Comm comm);

int MPI_Allreduce(const void* sendbuf,
                  void* recvbuf,
                  int count,
                  MPI_Datatype datatype,
                  MPI_Op op,
                  MPI_Comm comm);

int MPI_Scan(const void* sendbuf,
             void* recvbuf,
             int count,
             MPI_Datatype datatype,
             MPI_Op op,
             MPI_Comm comm);

int MPI_Alltoall(const void* sendbuf,
                 int sendcount,
                 MPI_Datatype sendtype,
                 void* recvbuf,
                 int recvcount,
                 MPI_Datatype recvtype,
                 MPI_Comm comm);

int MPI_Cart_create(MPI_Comm old_comm,
                    int ndims,
                    const int dims[],
                    const int periods[],
                    int reorder,
                    MPI_Comm* comm);

int MPI_Cart_rank(MPI_Comm comm, int coords[], int* rank);

int MPI_Cart_get(MPI_Comm comm,
                 int maxdims,
                 int dims[],
                 int periods[],
                 int coords[]);

int MPI_Cart_shift(MPI_Comm comm,
                   int direction,
                   int disp,
                   int* rank_source,
                   int* rank_dest);

int MPI_Type_size(MPI_Datatype type, int* size);

int MPI_Type_free(MPI_Datatype* datatype);

int MPI_Alloc_mem(MPI_Aint size, MPI_Info info, void* baseptr);

int MPI_Win_fence(int assert, MPI_Win win);

int MPI_Win_allocate_shared(MPI_Aint size,
                            int disp_unit,
                            MPI_Info info,
                            MPI_Comm comm,
                            void* baseptr,
                            MPI_Win* win);

int MPI_Win_shared_query(MPI_Win win,
                         int rank,
                         MPI_Aint* size,
                         int* disp_unit,
                         void* baseptr);

int MPI_Get(void* origin_addr,
            int origin_count,
            MPI_Datatype origin_datatype,
            int target_rank,
            MPI_Aint target_disp,
            int target_count,
            MPI_Datatype target_datatype,
            MPI_Win win);

int MPI_Put(const void* origin_addr,
            int origin_count,
            MPI_Datatype origin_datatype,
            int target_rank,
            MPI_Aint target_disp,
            int target_count,
            MPI_Datatype target_datatype,
            MPI_Win win);

int MPI_Win_free(MPI_Win* win);

int MPI_Win_create(void* base,
                   MPI_Aint size,
                   int disp_unit,
                   MPI_Info info,
                   MPI_Comm comm,
                   MPI_Win* win);

int MPI_Get_processor_name(char* name, int* resultlen);

int MPI_Win_get_attr(MPI_Win win,
                     int win_keyval,
                     void* attribute_val,
                     int* flag);

int MPI_Free_mem(void* base);

int MPI_Request_free(MPI_Request* request);

int MPI_Type_contiguous(int count,
                        MPI_Datatype oldtype,
                        MPI_Datatype* newtype);

int MPI_Type_commit(MPI_Datatype* type);

int MPI_Isend(const void* buf,
              int count,
              MPI_Datatype datatype,
              int dest,
              int tag,
              MPI_Comm comm,
              MPI_Request* request);

int MPI_Irecv(void* buf,
              int count,
              MPI_Datatype datatype,
              int source,
              int tag,
              MPI_Comm comm,
              MPI_Request* request);

double MPI_Wtime(void);

int MPI_Wait(MPI_Request* request, MPI_Status* status);

int MPI_Waitall(int count,
                MPI_Request array_of_requests[],
                MPI_Status* array_of_statuses);

int MPI_Waitany(int count,
                MPI_Request array_of_requests[],
                int* index,
                MPI_Status* status);

int MPI_Comm_create(MPI_Comm comm, MPI_Group group, MPI_Comm* newcomm);

int MPI_Comm_group(MPI_Comm comm, MPI_Group* group);

int MPI_Comm_dup(MPI_Comm comm, MPI_Comm* newcomm);

MPI_Fint MPI_Comm_c2f(MPI_Comm comm);

MPI_Comm MPI_Comm_f2c(MPI_Fint comm);

int MPI_Group_incl(MPI_Group group,
                   int n,
                   const int ranks[],
                   MPI_Group* newgroup);

int MPI_Comm_rank(MPI_Comm comm, int* rank);

int MPI_Comm_size(MPI_Comm comm, int* size);

int MPI_Comm_create_group(MPI_Comm comm,
                          MPI_Group group,
                          int tag,
                          MPI_Comm* newcomm);

int MPI_Comm_free(MPI_Comm* comm);

int MPI_Comm_split_type(MPI_Comm comm,
                        int split_type,
                        int key,
                        MPI_Info info,
                        MPI_Comm* newcomm);

int MPI_Comm_split(MPI_Comm comm, int color, int key, MPI_Comm* newcomm);

int MPI_Group_free(MPI_Group* group);

int MPI_Alltoallv(const void* sendbuf,
                  const int sendcounts[],
                  const int sdispls[],
                  MPI_Datatype sendtype,
                  void* recvbuf,
                  const int recvcounts[],
                  const int rdispls[],
                  MPI_Datatype recvtype,
                  MPI_Comm comm);

int MPI_Query_thread(int* provided);

int MPI_Init_thread(int* argc, char*** argv, int required, int* provided);

int MPI_Op_create(MPI_User_function* user_fn, int commute, MPI_Op* op);

int MPI_Op_free(MPI_Op* op);

