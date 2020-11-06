#ifndef FAABRIC_MPI_H
#define FAABRIC_MPI_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C"
{
#endif

#define MPI_SUCCESS 0
#define MPI_ERR_OTHER 1

    /**
     * Custom Faabric MPI implementation
     * Official MPI spec: https://www.mpi-forum.org/docs/
     * Open MPI repo: https://github.com/open-mpi/ompi
     */

#define MPI_MAX_OBJECT_NAME 128

    /*
     * Behind the scenes structs (some parts of which are defined in the MPI
     * specification) Each can be extended with private fields as necessary
     *
     * NOTE - be careful when passing these structs to and from WebAssembly. Any
     * datatypes with *different* sizes in 32-/64-bit space need to be
     * translated carefully
     */
    struct faabric_status_public_t
    {
        // These MPI_XXX fields are defined in the spec
        int MPI_SOURCE;
        int MPI_TAG;
        int MPI_ERROR;

        // These are internal
        int bytesSize;
    };

    // Open MPI version:
    // https://github.com/open-mpi/ompi/blob/master/ompi/datatype/ompi_datatype.h
    struct faabric_datatype_t
    {
        int id;
        int size;
    };

    // Open MPI version:
    // https://github.com/open-mpi/ompi/blob/master/ompi/communicator/communicator.h
    struct faabric_communicator_t
    {
        int id;
    };

    // Open MPI version:
    // https://github.com/open-mpi/ompi/blob/master/ompi/message/message.h
    struct faabric_message_t
    {
        int id;
    };

    // Open MPI version:
    // https://github.com/open-mpi/ompi/blob/master/ompi/win/win.h
    // TODO - can we globally uniquely identify a window from its world, rank
    // and size?
    struct faabric_win_t
    {
        int worldId;
        int rank;
        int size;
        int wasmPtr;
        int dispUnit;
    };

    struct faabric_op_t
    {
        int id;
    };

    struct faabric_info_t
    {
        int id;
    };

    struct faabric_request_t
    {
        int _unused;
    };

    struct faabric_group_t
    {
        int id;
    };

/**
 * User-facing constants
 */
// MPI_Comms
#define FAABRIC_COMM_WORLD 1
#define FAABRIC_COMM_NULL 2
    extern struct faabric_communicator_t faabric_comm_world;
    extern struct faabric_communicator_t faabric_comm_null;
#define MPI_COMM_WORLD &faabric_comm_world
#define MPI_COMM_NULL &faabric_comm_null

    // Communicator types
    enum
    {
        MPI_COMM_TYPE_SHARED,
    };

// Simple datatypes
#define FAABRIC_INT8 1
#define FAABRIC_INT16 2
#define FAABRIC_INT32 3
#define FAABRIC_INT 4
#define FAABRIC_INT64 5
#define FAABRIC_UINT8 6
#define FAABRIC_UINT16 7
#define FAABRIC_UINT32 8
#define FAABRIC_UINT 9
#define FAABRIC_UINT64 10
#define FAABRIC_LONG 11
#define FAABRIC_LONG_LONG 12
#define FAABRIC_LONG_LONG_INT 13
#define FAABRIC_FLOAT 14
#define FAABRIC_DOUBLE 15
#define FAABRIC_DOUBLE_INT 16
#define FAABRIC_CHAR 17
#define FAABRIC_C_BOOL 18
#define FAABRIC_BYTE 19
#define FAABRIC_DATATYPE_NULL 20
    extern struct faabric_datatype_t faabric_type_int8;
    extern struct faabric_datatype_t faabric_type_int16;
    extern struct faabric_datatype_t faabric_type_int32;
    extern struct faabric_datatype_t faabric_type_int;
    extern struct faabric_datatype_t faabric_type_int64;
    extern struct faabric_datatype_t faabric_type_uint8;
    extern struct faabric_datatype_t faabric_type_uint16;
    extern struct faabric_datatype_t faabric_type_uint32;
    extern struct faabric_datatype_t faabric_type_uint;
    extern struct faabric_datatype_t faabric_type_uint64;
    extern struct faabric_datatype_t faabric_type_long;
    extern struct faabric_datatype_t faabric_type_long_long;
    extern struct faabric_datatype_t faabric_type_long_long_int;
    extern struct faabric_datatype_t faabric_type_float;
    extern struct faabric_datatype_t faabric_type_double;
    extern struct faabric_datatype_t faabric_type_double_int;
    extern struct faabric_datatype_t faabric_type_char;
    extern struct faabric_datatype_t faabric_type_c_bool;
    extern struct faabric_datatype_t faabric_type_byte;
    extern struct faabric_datatype_t faabric_type_null;
#define MPI_INT8_T &faabric_type_int8
#define MPI_INT16_T &faabric_type_int16
#define MPI_INT32_T &faabric_type_int32
#define MPI_INT &faabric_type_int
#define MPI_INT64_T &faabric_type_int64
#define MPI_UINT8_T &faabric_type_uint8
#define MPI_UINT16_T &faabric_type_uint16
#define MPI_UINT32_T &faabric_type_uint32
#define MPI_UINT_T &faabric_type_uint
#define MPI_UINT64_T &faabric_type_uint64
#define MPI_LONG &faabric_type_long
#define MPI_LONG_LONG &faabric_type_long_long
#define MPI_LONG_LONG_INT &faabric_type_long_long_int
#define MPI_FLOAT &faabric_type_float
#define MPI_DOUBLE &faabric_type_double
#define MPI_DOUBLE_INT &faabric_type_double_int
#define MPI_CHAR &faabric_type_char
#define MPI_C_BOOL &faabric_type_c_bool
#define MPI_BYTE &faabric_type_byte
#define MPI_DATATYPE_NULL &faabric_type_null

    struct faabric_datatype_t* getFaabricDatatypeFromId(int datatypeId);

// MPI flags
// These are special pointers passed in place of normal buffers to signify
// special operations (e.g. in-place manipulations). We make the pointers
// themselves equal to a specific integer so that they can be identified.
#define FAABRIC_BOTTOM 1
#define FAABRIC_IN_PLACE 2
#define MPI_BOTTOM (void*)FAABRIC_BOTTOM
#define MPI_IN_PLACE (void*)FAABRIC_IN_PLACE

// MPI_Infos
#define FAABRIC_INFO_NULL 1
    extern struct faabric_info_t faabric_info_null;
#define MPI_INFO_NULL &faabric_info_null

// Misc constants
#define MPI_ANY_SOURCE -1

// Misc limits
#define MPI_MAX_PROCESSOR_NAME 256

// MPI_Ops
#define FAABRIC_OP_MAX 1
#define FAABRIC_OP_MIN 2
#define FAABRIC_OP_SUM 3
#define FAABRIC_OP_PROD 4
#define FAABRIC_OP_LAND 5
#define FAABRIC_OP_LOR 6
#define FAABRIC_OP_BAND 7
#define FAABRIC_OP_BOR 8
#define FAABRIC_OP_MAXLOC 9
#define FAABRIC_OP_MINLOC 10
#define FAABRIC_OP_NULL 11

    extern struct faabric_op_t faabric_op_max;
    extern struct faabric_op_t faabric_op_min;
    extern struct faabric_op_t faabric_op_sum;
    extern struct faabric_op_t faabric_op_prod;
    extern struct faabric_op_t faabric_op_land;
    extern struct faabric_op_t faabric_op_lor;
    extern struct faabric_op_t faabric_op_band;
    extern struct faabric_op_t faabric_op_bor;
    extern struct faabric_op_t faabric_op_maxloc;
    extern struct faabric_op_t faabric_op_minloc;
    extern struct faabric_op_t faabric_op_null;

#define MPI_MAX &faabric_op_max
#define MPI_MIN &faabric_op_min
#define MPI_SUM &faabric_op_sum
#define MPI_PROD &faabric_op_prod
#define MPI_LAND &faabric_op_land
#define MPI_LOR &faabric_op_lor
#define MPI_BAND &faabric_op_band
#define MPI_BOR &faabric_op_bor
#define MPI_MAXLOC &faabric_op_maxloc
#define MPI_MINLOC &faabric_op_minloc
#define MPI_OP_NULL &faabric_op_null

// MPI_Statuses
#define MPI_STATUS_IGNORE ((MPI_Status*)(0))
#define MPI_STATUSES_IGNORE ((MPI_Status*)(0))

// Window attributes
#define MPI_WIN_BASE 1
#define MPI_WIN_SIZE 2
#define MPI_WIN_DISP_UNIT 3
#define MPI_WIN_CREATE_FLAVOR 4
#define MPI_WIN_MODEL 5

    // MPI Threads
    enum
    {
        MPI_THREAD_SINGLE,
        MPI_THREAD_FUNNELED,
        MPI_THREAD_SERIALIZED,
        MPI_THREAD_MULTIPLE
    };

    /*
     * User-facing types
     */
    typedef struct faabric_op_t* MPI_Op;
    typedef struct faabric_communicator_t* MPI_Comm;
    typedef struct faabric_datatype_t* MPI_Datatype;
    typedef struct faabric_status_public_t MPI_Status;
    typedef struct faabric_message_t* MPI_Message;
    typedef struct faabric_info_t* MPI_Info;
    typedef struct faabric_request_t* MPI_Request;
    typedef struct faabric_group_t* MPI_Group;
    typedef struct faabric_win_t* MPI_Win;
    typedef ptrdiff_t MPI_Aint;
    typedef int MPI_Fint;
    typedef long MPI_Offset;

    /*
     * User-defined functions
     */
    typedef void(MPI_User_function)(void*, void*, int*, MPI_Datatype*);

    /*
     * User-facing functions
     */
    int MPI_Init(int* argc, char*** argv);

    int MPI_Initialized(int* flag);

    int MPI_Get_version(int* version, int* subversion);

    int MPI_Finalized(int* flag);

    int MPI_Finalize(void);

    int MPI_Send(const void* buf,
                 int count,
                 MPI_Datatype datatype,
                 int dest,
                 int tag,
                 MPI_Comm comm);

    int MPI_Rsend(const void* buf,
                  int count,
                  MPI_Datatype datatype,
                  int dest,
                  int tag,
                  MPI_Comm comm);

    int MPI_Recv(void* buf,
                 int count,
                 MPI_Datatype datatype,
                 int source,
                 int tag,
                 MPI_Comm comm,
                 MPI_Status* status);

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
                     MPI_Status* status);

    int MPI_Abort(MPI_Comm comm, int errorcode);

    int MPI_Get_count(const MPI_Status* status,
                      MPI_Datatype datatype,
                      int* count);

    int MPI_Probe(int source, int tag, MPI_Comm comm, MPI_Status* status);

    int MPI_Barrier(MPI_Comm comm);

    int MPI_Bcast(void* buffer,
                  int count,
                  MPI_Datatype datatype,
                  int root,
                  MPI_Comm comm);

    int MPI_Scatter(const void* sendbuf,
                    int sendcount,
                    MPI_Datatype sendtype,
                    void* recvbuf,
                    int recvcount,
                    MPI_Datatype recvtype,
                    int root,
                    MPI_Comm comm);

    int MPI_Gather(const void* sendbuf,
                   int sendcount,
                   MPI_Datatype sendtype,
                   void* recvbuf,
                   int recvcount,
                   MPI_Datatype recvtype,
                   int root,
                   MPI_Comm comm);

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

#ifdef __cplusplus
}
#endif

#endif
