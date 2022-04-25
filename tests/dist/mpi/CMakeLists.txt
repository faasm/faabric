# First we build the MPI native library
add_library(faabric_dist_tests_mpinative_lib
    mpi_native.cpp
    mpi_native.h
)

target_link_libraries(faabric_dist_tests_mpinative_lib PUBLIC faabric::test_utils)

# Second we set all the source files for the dist tests library
set(ALL_MPI_EXAMPLES
    ${CMAKE_CURRENT_LIST_DIR}/examples/mpi_allgather.cpp
    ${CMAKE_CURRENT_LIST_DIR}/examples/mpi_allreduce.cpp
    ${CMAKE_CURRENT_LIST_DIR}/examples/mpi_alltoall.cpp
    ${CMAKE_CURRENT_LIST_DIR}/examples/mpi_bcast.cpp
    ${CMAKE_CURRENT_LIST_DIR}/examples/mpi_cart_create.cpp
    ${CMAKE_CURRENT_LIST_DIR}/examples/mpi_cartesian.cpp
    ${CMAKE_CURRENT_LIST_DIR}/examples/mpi_checks.cpp
    ${CMAKE_CURRENT_LIST_DIR}/examples/mpi_gather.cpp
    ${CMAKE_CURRENT_LIST_DIR}/examples/mpi_helloworld.cpp
    ${CMAKE_CURRENT_LIST_DIR}/examples/mpi_isendrecv.cpp
    ${CMAKE_CURRENT_LIST_DIR}/examples/mpi_order.cpp
    ${CMAKE_CURRENT_LIST_DIR}/examples/mpi_reduce.cpp
    ${CMAKE_CURRENT_LIST_DIR}/examples/mpi_scan.cpp
    ${CMAKE_CURRENT_LIST_DIR}/examples/mpi_scatter.cpp
    ${CMAKE_CURRENT_LIST_DIR}/examples/mpi_send.cpp
    ${CMAKE_CURRENT_LIST_DIR}/examples/mpi_sendrecv.cpp
    ${CMAKE_CURRENT_LIST_DIR}/examples/mpi_status.cpp
    ${CMAKE_CURRENT_LIST_DIR}/examples/mpi_typesize.cpp
)

set(MPI_DIST_TEST_SOURCES
    ${CMAKE_CURRENT_LIST_DIR}/functions.cpp
    ${CMAKE_CURRENT_LIST_DIR}/mpi_native.h
    ${ALL_MPI_EXAMPLES}
    PARENT_SCOPE
)
