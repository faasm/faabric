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
    callMpiFunc(fmt::format("MPI_Sendrecv {} -> {} and {} <- {}",
                            executingContext.getRank(),
                            dest,
                            executingContext.getRank(),
                            source),
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
        logger->error("Incomplete message (bytes {}, datatype size {})",
                      status->bytesSize,
                      datatype->size);
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
                    (uint8_t*)buffer,
                    datatype,
                    count,
                    faabric::MPIMessage::NORMAL);
    } else {
        auto fptr = &faabric::scheduler::MpiWorld::recv;
        callMpiFunc(fmt::format("MPI_Bcast {} <- {}", rank, root),
                    fptr,
                    root,
                    rank,
                    (uint8_t*)buffer,
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
    callMpiFunc(
      fmt::format("MPI_Scatter {} -> {}", root, executingContext.getRank()),
      fptr,
      root,
      executingContext.getRank(),
      (uint8_t*)sendbuf,
      sendtype,
      sendcount,
      (uint8_t*)recvbuf,
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
    if (sendbuf == MPI_IN_PLACE) {
        sendbuf = recvbuf;
    }

    auto fptr = &faabric::scheduler::MpiWorld::gather;
    callMpiFunc("MPI_Gather",
                fptr,
                executingContext.getRank(),
                root,
                (uint8_t*)sendbuf,
                sendtype,
                sendcount,
                (uint8_t*)recvbuf,
                recvtype,
                recvcount);

    return MPI_SUCCESS;
}

int MPI_Allgather(const void* sendbuf,
                  int sendcount,
                  MPI_Datatype sendtype,
                  void* recvbuf,
                  int recvcount,
                  MPI_Datatype recvtype,
                  MPI_Comm comm)
{
    if (sendbuf == MPI_IN_PLACE) {
        sendbuf = recvbuf;
    }

    auto fptr = &faabric::scheduler::MpiWorld::allGather;
    callMpiFunc("MPI_Allgather",
                fptr,
                executingContext.getRank(),
                (uint8_t*)sendbuf,
                sendtype,
                sendcount,
                (uint8_t*)recvbuf,
                recvtype,
                recvcount);

    return MPI_SUCCESS;
}

int MPI_Allgatherv(const void* sendbuf,
                   int sendcount,
                   MPI_Datatype sendtype,
                   void* recvbuf,
                   const int* recvcounts,
                   const int* displs,
                   MPI_Datatype recvtype,
                   MPI_Comm comm)
{
    notImplemented("MPI_Allgatherv");

    return MPI_SUCCESS;
}

int MPI_Reduce(const void* sendbuf,
               void* recvbuf,
               int count,
               MPI_Datatype datatype,
               MPI_Op op,
               int root,
               MPI_Comm comm)
{
    if (sendbuf == MPI_IN_PLACE) {
        sendbuf = recvbuf;
    }

    auto fptr = &faabric::scheduler::MpiWorld::reduce;
    callMpiFunc("MPI_Reduce",
                fptr,
                executingContext.getRank(),
                root,
                (uint8_t*)sendbuf,
                (uint8_t*)recvbuf,
                datatype,
                count,
                op);

    return MPI_SUCCESS;
}

int MPI_Reduce_scatter(const void* sendbuf,
                       void* recvbuf,
                       const int* recvcounts,
                       MPI_Datatype datatype,
                       MPI_Op op,
                       MPI_Comm comm)
{
    notImplemented("MPI_Reduce_scatter");

    return MPI_SUCCESS;
}

int MPI_Allreduce(const void* sendbuf,
                  void* recvbuf,
                  int count,
                  MPI_Datatype datatype,
                  MPI_Op op,
                  MPI_Comm comm)
{
    if (sendbuf == MPI_IN_PLACE) {
        sendbuf = recvbuf;
    }

    auto fptr = &faabric::scheduler::MpiWorld::allReduce;
    callMpiFunc("MPI_Allreduce",
                fptr,
                executingContext.getRank(),
                (uint8_t*)sendbuf,
                (uint8_t*)recvbuf,
                datatype,
                count,
                op);

    return MPI_SUCCESS;
}

int MPI_Scan(const void* sendbuf,
             void* recvbuf,
             int count,
             MPI_Datatype datatype,
             MPI_Op op,
             MPI_Comm comm)
{
    if (sendbuf == MPI_IN_PLACE) {
        sendbuf = recvbuf;
    }

    auto fptr = &faabric::scheduler::MpiWorld::scan;
    callMpiFunc("MPI_Scan",
                fptr,
                executingContext.getRank(),
                (uint8_t*)sendbuf,
                (uint8_t*)recvbuf,
                datatype,
                count,
                op);

    return MPI_SUCCESS;
}

int MPI_Alltoall(const void* sendbuf,
                 int sendcount,
                 MPI_Datatype sendtype,
                 void* recvbuf,
                 int recvcount,
                 MPI_Datatype recvtype,
                 MPI_Comm comm)
{
    auto fptr = &faabric::scheduler::MpiWorld::allToAll;
    callMpiFunc("MPI_Alltoall",
                fptr,
                executingContext.getRank(),
                (uint8_t*)sendbuf,
                sendtype,
                sendcount,
                (uint8_t*)recvbuf,
                recvtype,
                recvcount);

    return MPI_SUCCESS;
}

int MPI_Cart_create(MPI_Comm old_comm,
                    int ndims,
                    const int dims[],
                    const int periods[],
                    int reorder,
                    MPI_Comm* comm)
{
    auto logger = faabric::util::getLogger();
    logger->debug("MPI_Cart_create");

    *comm = old_comm;

    return MPI_SUCCESS;
}

int MPI_Cart_rank(MPI_Comm comm, int coords[], int* rank)
{
    auto fptr = &faabric::scheduler::MpiWorld::getRankFromCoords;
    callMpiFunc("MPI_Cart_rank", fptr, rank, coords);

    return MPI_SUCCESS;
}

int MPI_Cart_get(MPI_Comm comm,
                 int maxdims,
                 int dims[],
                 int periods[],
                 int coords[])
{
    auto fptr = &faabric::scheduler::MpiWorld::getCartesianRank;
    callMpiFunc(
      "MPI_Cart_get", fptr, executingContext.getRank(), dims, periods, coords);

    return MPI_SUCCESS;
}

int MPI_Cart_shift(MPI_Comm comm,
                   int direction,
                   int disp,
                   int* rank_source,
                   int* rank_dest)
{
    auto logger = faabric::util::getLogger();
    logger->debug("MPI_Cart_shift");

    rank_source = rank_dest;

    return MPI_SUCCESS;
}

int MPI_Type_size(MPI_Datatype type, int* size)
{
    auto logger = faabric::util::getLogger();
    logger->debug("MPI_Type_size");

    *size = type->size;

    return MPI_SUCCESS;
}

int MPI_Type_free(MPI_Datatype* datatype)
{
    notImplemented("MPI_Type_free");

    return MPI_SUCCESS;
}

int MPI_Alloc_mem(MPI_Aint size, MPI_Info info, void* baseptr)
{
    auto logger = faabric::util::getLogger();
    logger->debug("MPI_Alloc_mem");

    if (info != MPI_INFO_NULL) {
        throw std::runtime_error("Non-null info not supported");
    }

    *((void**)baseptr) = malloc(size);

    return MPI_SUCCESS;
}

int MPI_Win_fence(int assert, MPI_Win win)
{
    auto fptr = &faabric::scheduler::MpiWorld::barrier;
    callMpiFunc("MPI_Win_fence", fptr, executingContext.getRank());

    return MPI_SUCCESS;
}

int MPI_Get(void* origin_addr,
            int origin_count,
            MPI_Datatype origin_datatype,
            int target_rank,
            MPI_Aint target_disp,
            int target_count,
            MPI_Datatype target_datatype,
            MPI_Win win)
{
    auto fptr = &faabric::scheduler::MpiWorld::rmaGet;
    callMpiFunc("MPI_Get",
                fptr,
                target_rank,
                target_datatype,
                target_count,
                (uint8_t*)origin_addr,
                origin_datatype,
                origin_count);

    return MPI_SUCCESS;
}

int MPI_Put(const void* origin_addr,
            int origin_count,
            MPI_Datatype origin_datatype,
            int target_rank,
            MPI_Aint target_disp,
            int target_count,
            MPI_Datatype target_datatype,
            MPI_Win win)
{
    auto fptr = &faabric::scheduler::MpiWorld::rmaPut;
    callMpiFunc("MPI_Put",
                fptr,
                executingContext.getRank(),
                (uint8_t*)origin_addr,
                origin_datatype,
                origin_count,
                target_rank,
                target_datatype,
                target_count);

    return MPI_SUCCESS;
}

int MPI_Win_free(MPI_Win* win)
{
    auto logger = faabric::util::getLogger();
    logger->debug("MPI_Win_free");

    return MPI_SUCCESS;
}

int MPI_Win_create(void* base,
                   MPI_Aint size,
                   int disp_unit,
                   MPI_Info info,
                   MPI_Comm comm,
                   MPI_Win* win)
{
    auto logger = faabric::util::getLogger();
    logger->debug("MPI_Win_create");
    faabric::scheduler::MpiWorld& world = getExecutingWorld();

    //(*win)->worldId = world.getId();
    (*win)->size = size;
    (*win)->dispUnit = disp_unit;
    (*win)->rank = executingContext.getRank();
    (*win)->basePtr = base;
    world.createWindow(*win, (uint8_t*)base);

    return MPI_SUCCESS;
}

int MPI_Get_processor_name(char* name, int* resultlen)
{
    auto logger = faabric::util::getLogger();
    logger->debug("MPI_Get_processor_name");

    std::string host = faabric::util::getSystemConfig().endpointHost;
    strncpy(name, host.c_str(), host.length());
    *resultlen = host.length();

    return MPI_SUCCESS;
}

int MPI_Win_get_attr(MPI_Win win,
                     int win_keyval,
                     void* attribute_val,
                     int* flag)
{
    auto logger = faabric::util::getLogger();
    logger->debug("MPI_Win_get_attr");

    *flag = 1;
    if (win_keyval == MPI_WIN_BASE) {
        *((void**)attribute_val) = win->basePtr;
    } else {
        if (win_keyval == MPI_WIN_SIZE) {
            *((int*)attribute_val) = win->size;
        } else if (win_keyval == MPI_WIN_DISP_UNIT) {
            *((int*)attribute_val) = win->dispUnit;
        } else {
            throw std::runtime_error("Unrecofnised window attribute type " +
                                     std::to_string(win_keyval));
        }
    }

    return MPI_SUCCESS;
}

int MPI_Free_mem(void* base)
{
    auto logger = faabric::util::getLogger();
    logger->debug("MPI_Win_get_attr");

    return MPI_SUCCESS;
}

int MPI_Request_free(MPI_Request* request)
{
    notImplemented("MPI_Request_free");

    return MPI_SUCCESS;
}

int MPI_Type_contiguous(int count, MPI_Datatype oldtype, MPI_Datatype* newtype)
{
    auto logger = faabric::util::getLogger();
    logger->debug("MPI_Type_contiguous");

    return MPI_SUCCESS;
}

int MPI_Type_commit(MPI_Datatype* type)
{
    auto logger = faabric::util::getLogger();
    logger->debug("MPI_Type_commit");

    return MPI_SUCCESS;
}

/*
 * The API call returns a request ID that we can't capture with the function
 * pointer trick. Thus we don't use it here.
 */
int MPI_Isend(const void* buf,
              int count,
              MPI_Datatype datatype,
              int dest,
              int tag,
              MPI_Comm comm,
              MPI_Request* request)
{
    auto logger = faabric::util::getLogger();
    logger->debug("MPI_Isend {} -> {}", executingContext.getRank(), dest);

    faabric::scheduler::MpiWorld& world = getExecutingWorld();
    int requestId = world.isend(
      executingContext.getRank(), dest, (uint8_t*)buf, datatype, count);
    (*request)->id = requestId;

    return MPI_SUCCESS;
}

/*
 * The API call returns a request ID that we can't capture with the function
 * pointer trick. Thus we don't use it here.
 */
int MPI_Irecv(void* buf,
              int count,
              MPI_Datatype datatype,
              int source,
              int tag,
              MPI_Comm comm,
              MPI_Request* request)
{
    auto logger = faabric::util::getLogger();
    logger->debug("MPI_Irecv {} <- {}", executingContext.getRank(), source);

    faabric::scheduler::MpiWorld& world = getExecutingWorld();
    int requestId = world.irecv(
      source, executingContext.getRank(), (uint8_t*)buf, datatype, count);
    (*request)->id = requestId;

    return MPI_SUCCESS;
}

double MPI_Wtime()
{
    auto fptr = &faabric::scheduler::MpiWorld::getWTimePtr;
    double time;
    callMpiFunc("MPI_Wtime", fptr, &time);

    return time;
}

int MPI_Wait(MPI_Request* request, MPI_Status* status)
{
    auto fptr = &faabric::scheduler::MpiWorld::awaitAsyncRequest;
    callMpiFunc("MPI_Wait", fptr, (*request)->id);

    return MPI_SUCCESS;
}

int MPI_Waitall(int count,
                MPI_Request array_of_requests[],
                MPI_Status* array_of_statuses)
{
    notImplemented("MPI_Waitall");

    return MPI_SUCCESS;
}

int MPI_Waitany(int count,
                MPI_Request array_of_requests[],
                int* index,
                MPI_Status* status)
{
    notImplemented("MPI_Waitany");

    return MPI_SUCCESS;
}

int MPI_Comm_dup(MPI_Comm comm, MPI_Comm* newcomm)
{
    notImplemented("MPI_Comm_dup");

    return MPI_SUCCESS;
}

MPI_Fint MPI_Comm_c2f(MPI_Comm comm)
{
    notImplemented("MPI_Comm_c2f");

    return MPI_SUCCESS;
}

MPI_Comm MPI_Comm_f2c(MPI_Fint comm)
{
    notImplemented("MPI_Comm_f2c");

    return MPI_SUCCESS;
}

int MPI_Comm_free(MPI_Comm* comm)
{
    auto logger = faabric::util::getLogger();
    logger->debug("MPI_Comm_free");

    return MPI_SUCCESS;
}

int MPI_Comm_split(MPI_Comm comm, int color, int key, MPI_Comm* newcomm)
{
    notImplemented("MPI_Comm_split");

    return MPI_SUCCESS;
}

int MPI_Alltoallv(const void* sendbuf,
                  const int sendcounts[],
                  const int sdispls[],
                  MPI_Datatype sendtype,
                  void* recvbuf,
                  const int recvcounts[],
                  const int rdispls[],
                  MPI_Datatype recvtype,
                  MPI_Comm comm)
{
    notImplemented("MPI_Alltoallv");

    return MPI_SUCCESS;
}

int MPI_Op_create(MPI_User_function* user_fn, int commute, MPI_Op* op)
{
    notImplemented("MPI_Op_create");

    return MPI_SUCCESS;
}

int MPI_Op_free(MPI_Op* op)
{
    notImplemented("MPI_Op_free");

    return MPI_SUCCESS;
}
