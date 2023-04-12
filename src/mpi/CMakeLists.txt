cmake_minimum_required(VERSION 3.13.0)
project(faabricmpi)

# -----------------------------------------------
# Standalone Faabric MPI library
# -----------------------------------------------

set(FAABRIC_INCLUDE_DIR ${CMAKE_CURRENT_LIST_DIR}/../../include)

include_directories(${FAABRIC_INCLUDE_DIR})

set(PUBLIC_HEADERS
        ${FAABRIC_INCLUDE_DIR}/faabric/mpi/mpi.h
    )


add_library(faabricmpi mpi.cpp ${PUBLIC_HEADERS})
add_library(faabric::faabricmpi ALIAS faabricmpi)

set_target_properties(faabricmpi
    PROPERTIES PUBLIC_HEADER "${PUBLIC_HEADERS}"
    )

if(CMAKE_SYSTEM_NAME STREQUAL "Wasm")
    message(STATUS "Faabric MPI WebAssembly build")
    set(LIB_DIRECTORY ${CMAKE_SYSROOT}/lib/wasm32-wasi)

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


# -----------------------------------------------
# MPI Implementation for Faabric
# -----------------------------------------------

if ("${CMAKE_PROJECT_NAME}" STREQUAL "faabric")
    message(STATUS "PROJECT NAME: ${CMAKE_PROJECT_NAME}")
    faabric_lib(mpi
        MpiContext.cpp
        MpiMessageBuffer.cpp
        MpiWorld.cpp
        MpiWorldRegistry.cpp
    )

    target_link_libraries(mpi PRIVATE
        faabric::scheduler
        faabric::faabricmpi
    )
endif ()
