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

# Global include dir
set(FAABRIC_INCLUDE_DIR ${CMAKE_CURRENT_LIST_DIR}/include)

# Output directories
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)

# External libraries
include(cmake/ExternalProjects.cmake)

# Library funcs
function(faabric_lib lib_name lib_deps)
    add_library(${lib_name} ${lib_deps})

    add_library(${lib_name}_obj OBJECT ${lib_deps})

    # Include directories
    target_include_directories(${lib_name} 
        PUBLIC ${FAABRIC_INCLUDE_DIR}
        PUBLIC ${PISTACHE_INCLUDE_DIR}
        PUBLIC ${SPDLOG_INCLUDE_DIR}
        PUBLIC ${RAPIDJSON_INCLUDE_DIR}
        )

    if(BUILD_SHARED_LIBS)
        target_compile_options(${lib_name} PRIVATE "-fPIC")
        target_compile_options(${lib_name}_obj PRIVATE "-fPIC")
    endif()    
endfunction()

add_subdirectory(src/endpoint)
add_subdirectory(src/executor)
add_subdirectory(src/mpi)
add_subdirectory(src/proto)
add_subdirectory(src/redis)
add_subdirectory(src/scheduler)
add_subdirectory(src/state)
add_subdirectory(src/util)

# Wrapper library - note we want to include all the targets in this library, not
# just link it superficially
if(BUILD_SHARED_LIBS) 
    set(FAABRIC_LIB_TYPE SHARED)
else()
    set(FAABRIC_LIB_TYPE STATIC)
endif()

add_library(faabric 
    ${FAABRIC_LIB_TYPE}
    faabric.cpp
    $<TARGET_OBJECTS:endpoint_obj>
    $<TARGET_OBJECTS:executor_obj>
    $<TARGET_OBJECTS:proto_obj>
    $<TARGET_OBJECTS:redis_obj>
    $<TARGET_OBJECTS:scheduler_obj>
    $<TARGET_OBJECTS:state_obj>
    $<TARGET_OBJECTS:util_obj>
)

add_dependencies(faabric pistache_ext)

target_link_libraries(faabric
    ${Protobuf_LIBRARIES}
    gRPC::grpc++
    gRPC::grpc++_reflection
    hiredis
    boost_system
    boost_filesystem
    )

target_include_directories(faabric PUBLIC 
    ${FAABRIC_INCLUDE_DIR}
    ${CMAKE_INSTALL_PREFIX}/include
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
