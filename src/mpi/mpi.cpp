#include "faabric/mpi/mpi.h"

#include <stdexcept>
#include <stdint.h>

struct faabric_communicator_t faabric_comm_world{.id=FAABRIC_COMM_WORLD};

struct faabric_datatype_t faabric_type_int8{.id=FAABRIC_INT8, .size=sizeof(int8_t)};
struct faabric_datatype_t faabric_type_int16{.id=FAABRIC_INT16, .size=sizeof(int16_t)};
struct faabric_datatype_t faabric_type_int32{.id=FAABRIC_INT32, .size=sizeof(int32_t)};
struct faabric_datatype_t faabric_type_int{.id=FAABRIC_INT, .size=sizeof(int32_t)};
struct faabric_datatype_t faabric_type_int64{.id=FAABRIC_INT64, .size=sizeof(int64_t)};

struct faabric_datatype_t faabric_type_uint8{.id=FAABRIC_UINT8, .size=sizeof(uint8_t)};
struct faabric_datatype_t faabric_type_uint16{.id=FAABRIC_UINT16, .size=sizeof(uint16_t)};
struct faabric_datatype_t faabric_type_uint32{.id=FAABRIC_UINT32, .size=sizeof(uint32_t)};
struct faabric_datatype_t faabric_type_uint{.id=FAABRIC_UINT, .size=sizeof(uint32_t)};
struct faabric_datatype_t faabric_type_uint64{.id=FAABRIC_UINT64, .size=sizeof(uint64_t)};

struct faabric_datatype_t faabric_type_long{.id=FAABRIC_LONG, .size=sizeof(long)};
struct faabric_datatype_t faabric_type_long_long_int{.id=FAABRIC_LONG_LONG_INT, .size=sizeof(long long int)};

struct faabric_datatype_t faabric_type_float{.id=FAABRIC_FLOAT, .size=sizeof(float)};
struct faabric_datatype_t faabric_type_double{.id=FAABRIC_DOUBLE, .size=sizeof(double)};
struct faabric_datatype_t faabric_type_char{.id=FAABRIC_CHAR, .size=sizeof(char)};
struct faabric_datatype_t faabric_type_c_bool{.id=FAABRIC_C_BOOL, .size=sizeof(bool)};
struct faabric_datatype_t faabric_type_byte{.id=FAABRIC_BYTE, .size=sizeof(char)};
struct faabric_datatype_t faabric_type_null{.id=FAABRIC_DATATYPE_NULL, .size=1};

struct faabric_info_t faabric_info_null{.id=FAABRIC_INFO_NULL};

struct faabric_op_t faabric_op_max{.id=FAABRIC_OP_MAX};
struct faabric_op_t faabric_op_min{.id=FAABRIC_OP_MIN};
struct faabric_op_t faabric_op_sum{.id=FAABRIC_OP_SUM};
struct faabric_op_t faabric_op_prod{.id=FAABRIC_OP_PROD};
struct faabric_op_t faabric_op_land{.id=FAABRIC_OP_LAND};
struct faabric_op_t faabric_op_lor{.id=FAABRIC_OP_LOR};
struct faabric_op_t faabric_op_band{.id=FAABRIC_OP_BAND};
struct faabric_op_t faabric_op_bor{.id=FAABRIC_OP_BOR};
struct faabric_op_t faabric_op_maxloc{.id=FAABRIC_OP_MAXLOC};
struct faabric_op_t faabric_op_minloc{.id=FAABRIC_OP_MINLOC};

faabric_datatype_t *getFaabricDatatypeFromId(int datatypeId) {
    switch (datatypeId) {
        case FAABRIC_INT8:
            return MPI_INT8_T;
        case FAABRIC_INT16:
            return MPI_INT16_T;
        case FAABRIC_INT32:
            return MPI_INT32_T;
        case FAABRIC_INT:
            return MPI_INT;
        case FAABRIC_INT64:
            return MPI_INT64_T;
        case FAABRIC_UINT8:
            return MPI_UINT8_T;
        case FAABRIC_UINT16:
            return MPI_UINT16_T;
        case FAABRIC_UINT32:
            return MPI_UINT32_T;
        case FAABRIC_UINT:
            return MPI_UINT_T;
        case FAABRIC_UINT64:
            return MPI_UINT64_T;
        case FAABRIC_LONG:
            return MPI_LONG;
        case FAABRIC_LONG_LONG_INT:
            return MPI_LONG_LONG_INT;
        case FAABRIC_FLOAT:
            return MPI_FLOAT;
        case FAABRIC_DOUBLE:
            return MPI_DOUBLE;
        case FAABRIC_CHAR:
            return MPI_CHAR;
        case FAABRIC_BYTE:
            return MPI_BYTE;
        default:
            throw std::runtime_error("Unrecognised datatype ID\n");
    }
}

