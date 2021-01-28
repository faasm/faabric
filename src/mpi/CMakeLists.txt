cmake_minimum_required(VERSION 3.13.0)
project(faabricmpi)

# -----------------------------------------------
# This library must support being compiled on its own
# -----------------------------------------------

set(FAABRIC_INCLUDE_DIR ${CMAKE_CURRENT_LIST_DIR}/../../include)

include_directories(${FAABRIC_INCLUDE_DIR})

set(PUBLIC_HEADERS
        ${FAABRIC_INCLUDE_DIR}/faabric/mpi/mpi.h
    )

if(BUILD_SHARED_LIBS)
    add_library(faabricmpi SHARED mpi.cpp ${PUBLIC_HEADERS})
else()
    add_library(faabricmpi STATIC mpi.cpp ${PUBLIC_HEADERS})
endif()

set_target_properties(faabricmpi 
    PROPERTIES PUBLIC_HEADER "${PUBLIC_HEADERS}"
    )

if(CMAKE_SYSTEM_NAME STREQUAL "Wasm")
    message(STATUS "Faabric MPI WebAssembly build")
    set(LIB_DIRECTORY ${CMAKE_SYSROOT}/lib/wasm32-wasi)

    add_compile_definitions(__wasm__)
    
    install(TARGETS faabricmpi
        ARCHIVE DESTINATION ${LIB_DIRECTORY}
        LIBRARY DESTINATION ${LIB_DIRECTORY}
        PUBLIC_HEADER DESTINATION ${CMAKE_SYSROOT}/include/faabric/mpi
    )
else()
    message(STATUS "Faabric MPI native build")
    install(TARGETS faabricmpi
        PUBLIC_HEADER DESTINATION ${CMAKE_INSTALL_PREFIX}/include/faabric/mpi
    )
endif()

