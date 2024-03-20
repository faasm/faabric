cmake_minimum_required(VERSION 3.13.0)
project(faabricmpi)

# -----------------------------------------------
# Standalone Faabric MPI library
# -----------------------------------------------

set(FAABRICMPI_INCLUDE_DIR ${CMAKE_CURRENT_LIST_DIR}/../../include)
set(FAABRICMPI_PUBLIC_HEADERS ${FAABRICMPI_INCLUDE_DIR}/faabric/mpi/mpi.h)

add_library(faabricmpi mpi.cpp ${FAABRICMPI_PUBLIC_HEADERS})
target_include_directories(faabricmpi PUBLIC ${FAABRICMPI_INCLUDE_DIR})
add_library(faabric::faabricmpi ALIAS faabricmpi)

set_target_properties(faabricmpi
    PROPERTIES PUBLIC_HEADER "${FAABRICMPI_PUBLIC_HEADERS}"
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

if (NOT ("${CMAKE_PROJECT_NAME}" STREQUAL "faabricmpi"))
    faabric_lib(mpi
        MpiContext.cpp
        MpiMessage.cpp
        MpiWorld.cpp
        MpiWorldRegistry.cpp
    )

    target_link_libraries(mpi PRIVATE
        faabric::scheduler
        faabric::faabricmpi
    )
endif ()
