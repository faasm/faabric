cmake_minimum_required(VERSION 3.13.0)
project(faabric)

option(FAABRIC_WASM_BUILD "Build Faabric wasm library" OFF)
option(FAABRIC_BUILD_TESTS "Build Faabric tests" ON)

# Top-level CMake config
set(CMAKE_CXX_FLAGS_DEBUG "-g")
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# Compile comamnds for clang tools
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# Third party deps
set(THIRD_PARTY_INSTALL_DIR ${CMAKE_BINARY_DIR}/third-party)

# Library funcs
if (BUILD_SHARED_LIBS)
    function(faabric_public_lib lib_name lib_deps)
        add_library(${lib_name} SHARED ${lib_deps})
    endfunction()
    function(faabric_private_lib lib_name lib_deps)
        add_library(${lib_name} STATIC ${lib_deps})
        target_compile_options(${lib_name} PRIVATE "-fPIC")
    endfunction()
else ()
    function(faabric_public_lib lib_name lib_deps)
        add_library(${lib_name} STATIC ${lib_deps})
    endfunction()
    function(faabric_private_lib lib_name lib_deps)
        add_library(${lib_name} STATIC ${lib_deps})
    endfunction()
endif ()

# Output directories
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)

# External libraries
include(cmake/ExternalProjects.cmake)

# Global include dir
set(FAABRIC_INCLUDE_DIR ${CMAKE_CURRENT_LIST_DIR}/include)

# Include directories
include_directories(
    ${FAABRIC_INCLUDE_DIR}
    ${CMAKE_CURRENT_BINARY_DIR}/src # Required for protobuf
)

add_subdirectory(src/endpoint)
add_subdirectory(src/executor)
add_subdirectory(src/mpi)
add_subdirectory(src/proto)
add_subdirectory(src/redis)
add_subdirectory(src/scheduler)
add_subdirectory(src/state)
add_subdirectory(src/util)

add_subdirectory(examples)

# Wrapper library
faabric_public_lib(faabric faabric.cpp)

target_link_libraries(faabric
        endpoint
        executor
        proto
        redis
        scheduler
        state
        util
        )

target_include_directories(faabric PUBLIC 
    ${FAABRIC_INCLUDE_DIR}
    ${THIRD_PARTY_INSTALL_DIR}/include
)

# Tests
if(FAABRIC_BUILD_TESTS)
    add_subdirectory(tests/test)
    add_subdirectory(tests/utils)
endif()

# Install headers
install(
    DIRECTORY ${FAABRIC_INCLUDE_DIR}/faabric
    DESTINATION include
)

install(TARGETS faabric)
