#include "mpi_native.h"

#include <faabric/mpi/mpi.h>
#include <faabric/scheduler/MpiContext.h>
#include <faabric/scheduler/MpiWorld.h>
#include <faabric/util/config.h>
#include <faabric/util/logging.h>

static thread_local faabric::scheduler::MpiContext executingContext;

faabric::Message* getExecutingCall()
{
    if (tests::mpi::executingCall == nullptr) {
        SPDLOG_ERROR("Null-pointing executing call in MPI native");
        throw std::runtime_error("Executing call not set");
    }
    return tests::mpi::executingCall;
}

faabric::scheduler::MpiWorld& getExecutingWorld()
{
    faabric::scheduler::MpiWorldRegistry& reg =
      faabric::scheduler::getMpiWorldRegistry();
    return reg.getOrInitialiseWorld(*getExecutingCall());
}

static void notImplemented(const std::string& funcName)
{
    SPDLOG_DEBUG("MPI - {}", funcName);

    throw std::runtime_error(funcName + " not implemented.");
}

int terminateMpi()
{
    // Destroy the MPI world
    getExecutingWorld().destroy();

    return MPI_SUCCESS;
}


int MPI_Init(int* argc, char*** argv)
{
    faabric::Message* call = getExecutingCall();

    if (call->mpirank() <= 0) {
        SPDLOG_DEBUG("MPI - MPI_Init (create)");

        int worldId = executingContext.createWorld(*call);
        call->set_mpiworldid(worldId);
    } else {
        SPDLOG_DEBUG("MPI - MPI_Init (join)");
        executingContext.joinWorld(*call);
    }

    // Initialise MPI-specific logging
    int thisRank = executingContext.getRank();
    SPDLOG_DEBUG("Initialised world (id: {}) for rank: {}", call->mpiworldid(), thisRank);

    return MPI_SUCCESS;
}

int MPI_Comm_rank(MPI_Comm comm, int* rank)
{
    SPDLOG_DEBUG("MPI - MPI_Comm_rank");

    *rank = executingContext.getRank();

    return MPI_SUCCESS;
}

int MPI_Comm_size(MPI_Comm comm, int* size)
{
    SPDLOG_DEBUG("MPI - MPI_Comm_size");
    *size = getExecutingWorld().getSize();

    return MPI_SUCCESS;
}

int MPI_Finalize()
{
    SPDLOG_DEBUG("MPI - MPI_Finalize");

    return terminateMpi();
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
    SPDLOG_DEBUG(
      fmt::format("MPI_Send {} -> {}", executingContext.getRank(), dest));
    getExecutingWorld().send(executingContext.getRank(),
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
    SPDLOG_DEBUG(
      fmt::format("MPI_Recv {} <- {}", executingContext.getRank(), source));
    getExecutingWorld().recv(source,
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
    SPDLOG_DEBUG(fmt::format("MPI_Sendrecv {} -> {} and {} <- {}",
                             executingContext.getRank(),
                             dest,
                             executingContext.getRank(),
                             source));
    getExecutingWorld().sendRecv((uint8_t*)sendbuf,
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
    SPDLOG_DEBUG("MPI - MPI_Abort");

    return terminateMpi();
}

int MPI_Get_count(const MPI_Status* status, MPI_Datatype datatype, int* count)
{
    SPDLOG_DEBUG("MPI - MPI_Get_count");

    if (status->bytesSize % datatype->size != 0) {
        SPDLOG_ERROR("Incomplete message (bytes {}, datatype size {})",
                     status->bytesSize,
                     datatype->size);
        return 1;
    }

    *count = status->bytesSize / datatype->size;

    return MPI_SUCCESS;
}

int MPI_Probe(int source, int tag, MPI_Comm comm, MPI_Status* status)
{
    SPDLOG_DEBUG("MPI - MPI_Probe");
    getExecutingWorld().probe(source, executingContext.getRank(), status);

    return MPI_SUCCESS;
}

int MPI_Barrier(MPI_Comm comm)
{
    SPDLOG_DEBUG("MPI - MPI_Barrier");
    getExecutingWorld().barrier(executingContext.getRank());

    return MPI_SUCCESS;
}

int MPI_Bcast(void* buffer,
              int count,
              MPI_Datatype datatype,
              int root,
              MPI_Comm comm)
{
    /*
    faabric::scheduler::MpiWorld& world = getExecutingWorld();

    int rank = executingContext.getRank();
    if (rank == root) {
        SPDLOG_DEBUG(fmt::format("MPI_Bcast {} -> all", rank));
        world.broadcast(
          rank, (uint8_t*)buffer, datatype, count, faabric::MPIMessage::NORMAL);
    } else {
        SPDLOG_DEBUG(fmt::format("MPI_Bcast {} <- {}", rank, root));
        world.recv(root,
                   rank,
                   (uint8_t*)buffer,
                   datatype,
                   count,
                   nullptr,
                   faabric::MPIMessage::NORMAL);
    }
    return MPI_SUCCESS;
    */
    // TODO - copy new implementation from Faasm
    throw std::runtime_error("MPI_Bcast not implemented");
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
    SPDLOG_DEBUG(
      fmt::format("MPI_Scatter {} -> {}", root, executingContext.getRank()));
    getExecutingWorld().scatter(root,
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

    SPDLOG_DEBUG("MPI - MPI_Gather");
    getExecutingWorld().gather(executingContext.getRank(),
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

    SPDLOG_DEBUG("MPI - MPI_Allgather");
    getExecutingWorld().allGather(executingContext.getRank(),
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

    SPDLOG_DEBUG("MPI - MPI_Reduce");
    getExecutingWorld().reduce(executingContext.getRank(),
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

    SPDLOG_DEBUG("MPI - MPI_Allreduce");
    getExecutingWorld().allReduce(executingContext.getRank(),
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

    SPDLOG_DEBUG("MPI - MPI_Scan");
    getExecutingWorld().scan(executingContext.getRank(),
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
    SPDLOG_DEBUG("MPI - MPI_Alltoall");
    getExecutingWorld().allToAll(executingContext.getRank(),
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
    SPDLOG_DEBUG("MPI - MPI_Cart_create");

    *comm = old_comm;

    return MPI_SUCCESS;
}

int MPI_Cart_rank(MPI_Comm comm, int coords[], int* rank)
{
    SPDLOG_DEBUG("MPI - MPI_Cart_rank");
    getExecutingWorld().getRankFromCoords(rank, coords);

    return MPI_SUCCESS;
}

int MPI_Cart_get(MPI_Comm comm,
                 int maxdims,
                 int dims[],
                 int periods[],
                 int coords[])
{
    SPDLOG_DEBUG("MPI - MPI_Cart_get");
    getExecutingWorld().getCartesianRank(
      executingContext.getRank(), maxdims, dims, periods, coords);

    return MPI_SUCCESS;
}

int MPI_Cart_shift(MPI_Comm comm,
                   int direction,
                   int disp,
                   int* rank_source,
                   int* rank_dest)
{
    SPDLOG_DEBUG("MPI - MPI_Cart_shift");

    getExecutingWorld().shiftCartesianCoords(
      executingContext.getRank(), direction, disp, rank_source, rank_dest);

    return MPI_SUCCESS;
}

int MPI_Type_size(MPI_Datatype type, int* size)
{
    SPDLOG_DEBUG("MPI - MPI_Type_size");

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
    SPDLOG_DEBUG("MPI - MPI_Alloc_mem");

    if (info != MPI_INFO_NULL) {
        throw std::runtime_error("Non-null info not supported");
    }

    *((void**)baseptr) = malloc(size);

    return MPI_SUCCESS;
}

int MPI_Win_fence(int assert, MPI_Win win)
{
    notImplemented("MPI_Win_fence");

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
    notImplemented("MPI_Get");

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
    notImplemented("MPI_Put");

    return MPI_SUCCESS;
}

int MPI_Win_free(MPI_Win* win)
{
    notImplemented("MPI_Win_free");

    return MPI_SUCCESS;
}

int MPI_Win_create(void* base,
                   MPI_Aint size,
                   int disp_unit,
                   MPI_Info info,
                   MPI_Comm comm,
                   MPI_Win* win)
{
    notImplemented("MPI_Win_create");

    return MPI_SUCCESS;
}

int MPI_Get_processor_name(char* name, int* resultlen)
{
    SPDLOG_DEBUG("MPI - MPI_Get_processor_name");

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
    SPDLOG_DEBUG("MPI - MPI_Win_get_attr");

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
    SPDLOG_DEBUG("MPI - MPI_Free_mem");

    return MPI_SUCCESS;
}

int MPI_Request_free(MPI_Request* request)
{
    notImplemented("MPI_Request_free");

    return MPI_SUCCESS;
}

int MPI_Type_contiguous(int count, MPI_Datatype oldtype, MPI_Datatype* newtype)
{
    SPDLOG_DEBUG("MPI - MPI_Type_contiguous");

    return MPI_SUCCESS;
}

int MPI_Type_commit(MPI_Datatype* type)
{
    SPDLOG_DEBUG("MPI - MPI_Type_commit");

    return MPI_SUCCESS;
}

int MPI_Isend(const void* buf,
              int count,
              MPI_Datatype datatype,
              int dest,
              int tag,
              MPI_Comm comm,
              MPI_Request* request)
{
    SPDLOG_DEBUG("MPI - MPI_Isend {} -> {}", executingContext.getRank(), dest);

    faabric::scheduler::MpiWorld& world = getExecutingWorld();
    (*request) = (faabric_request_t*)malloc(sizeof(faabric_request_t));
    int requestId = world.isend(
      executingContext.getRank(), dest, (uint8_t*)buf, datatype, count);
    (*request)->id = requestId;

    return MPI_SUCCESS;
}

int MPI_Irecv(void* buf,
              int count,
              MPI_Datatype datatype,
              int source,
              int tag,
              MPI_Comm comm,
              MPI_Request* request)
{
    SPDLOG_DEBUG(
      "MPI - MPI_Irecv {} <- {}", executingContext.getRank(), source);

    faabric::scheduler::MpiWorld& world = getExecutingWorld();
    (*request) = (faabric_request_t*)malloc(sizeof(faabric_request_t));
    int requestId = world.irecv(
      source, executingContext.getRank(), (uint8_t*)buf, datatype, count);
    (*request)->id = requestId;

    return MPI_SUCCESS;
}

double MPI_Wtime()
{
    SPDLOG_DEBUG("MPI - MPI_Wtime");

    return getExecutingWorld().getWTime();
}

int MPI_Wait(MPI_Request* request, MPI_Status* status)
{
    SPDLOG_DEBUG("MPI - MPI_Wait");
    getExecutingWorld().awaitAsyncRequest((*request)->id);

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
    SPDLOG_DEBUG("MPI - MPI_Comm_free");

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
