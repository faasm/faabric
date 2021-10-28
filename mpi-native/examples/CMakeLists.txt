cmake_minimum_required(VERSION 3.21.0)
project(faabric-mpi-examples)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# -----------------------------------------------
# Separate build for MPI-related examples. These are used for testing purposes.
# -----------------------------------------------
set(FAABRIC_LIB_DIR "/build/faabric/install/lib")

include(../../cmake/ExternalProjects.cmake)

function(add_example example_name)
    add_executable(${example_name} ${example_name}.cpp)

    target_link_libraries(${example_name}
        ${FAABRIC_LIB_DIR}/libfaabric.so
        ${FAABRIC_LIB_DIR}/libfaabricmpi.so
        ${FAABRIC_LIB_DIR}/libfaabricmpi_native.so
        faabric::common_dependencies
    )

    set_property(TARGET ${example_name} PROPERTY BUILD_RPATH "${FAABRIC_LIB_DIR}")

    set(ALL_EXAMPLES ${ALL_EXAMPLES} ${example_name} PARENT_SCOPE)
endfunction()

add_example(mpi_allgather)
add_example(mpi_allreduce)
add_example(mpi_alltoall)
add_example(mpi_bcast)
add_example(mpi_cart_create)
add_example(mpi_cartesian)
add_example(mpi_checks)
add_example(mpi_gather)
add_example(mpi_helloworld)
add_example(mpi_isendrecv)
add_example(mpi_onesided)
add_example(mpi_order)
add_example(mpi_probe)
add_example(mpi_reduce)
add_example(mpi_scan)
add_example(mpi_scatter)
add_example(mpi_send)
add_example(mpi_sendrecv)
add_example(mpi_status)
add_example(mpi_typesize)
add_example(mpi_wincreate)

add_custom_target(all_examples DEPENDS ${ALL_EXAMPLES})
