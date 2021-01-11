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

if(BUILD_SHARED_LIBS)
    add_library(faabricmpi_native SHARED ${LIB_FILES})
else()
    add_library(faabricmpi_native STATIC ${LIB_FILES})
endif()

set_target_properties(faabricmpi_native
    PROPERTIES PUBLIC_HEADER "${MPI_NATIVE_HEADERS}"
)

target_link_libraries(faabricmpi_native 
    faabric
    faabricmpi
    /build/faabric/install/lib/libpistache.so
)

install(TARGETS faabricmpi_native
    PUBLIC_HEADER DESTINATION ${CMAKE_INSTALL_PREFIX}/include/faabric/mpi-native
)
