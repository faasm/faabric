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
)

set(MPI_DIST_TEST_SOURCES
    ${CMAKE_CURRENT_LIST_DIR}/functions.cpp
    ${CMAKE_CURRENT_LIST_DIR}/mpi_native.h
    ${ALL_MPI_EXAMPLES}
    PARENT_SCOPE
)
