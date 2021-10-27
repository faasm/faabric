cmake_minimum_required(VERSION 3.13.0)
project(faabricmpi_native)

# -----------------------------------------------
# This library must support being compiled on its own
# -----------------------------------------------

message(STATUS "Faabric native MPI implementation")

set(FAABRIC_INCLUDE_DIR ${CMAKE_CURRENT_LIST_DIR}/../../include)

include_directories(${FAABRIC_INCLUDE_DIR})

file(GLOB MPI_NATIVE_HEADERS "${FAABRIC_INCLUDE_DIR}/faabric/mpi-native/*.h")

set(LIB_FILES
        mpi_native.cpp
        MpiExecutor.cpp
        ${MPI_NATIVE_HEADERS}
    )

add_library(faabricmpi_native ${LIB_FILES})
add_library(faabric::faabricmpi_native ALIAS faabricmpi_native)

set_target_properties(faabricmpi_native
    PROPERTIES PUBLIC_HEADER "${MPI_NATIVE_HEADERS}"
)

target_include_directories(faabricmpi_native
    PUBLIC /build/faabric/install/include
    )

set(FAABRIC_LIB_DIR "/build/faabric/install/lib")
target_link_libraries(faabricmpi_native PUBLIC
    faabric::faabric
    faabric::faabricmpi
)

install(TARGETS faabricmpi_native
    PUBLIC_HEADER DESTINATION ${CMAKE_INSTALL_PREFIX}/include/faabric/mpi-native
)
