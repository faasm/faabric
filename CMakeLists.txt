cmake_minimum_required(VERSION 3.13.0)
project(faabric)

option(FAABRIC_WASM_BUILD "Build Faabric wasm library" OFF)
option(FAABRIC_STATIC_LIBS "Statically link Faabric libs" ON)
option(FAABRIC_BUILD_TESTS "Build Faabric tests" ON)

# Top-level CMake config
set(CMAKE_CXX_FLAGS_DEBUG "-g")
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# Library funcs
if (FAABRIC_STATIC_LIBS)
    function(faabric_public_lib lib_name lib_deps)
        add_library(${lib_name} STATIC ${lib_deps})
    endfunction()
    function(faabric_private_lib lib_name lib_deps)
        add_library(${lib_name} STATIC ${lib_deps})
    endfunction()
else ()
    function(faabric_public_lib lib_name lib_deps)
        add_library(${lib_name} SHARED ${lib_deps})
    endfunction()
    function(faabric_private_lib lib_name lib_deps)
        add_library(${lib_name} STATIC ${lib_deps})
        target_compile_options(${lib_name} PRIVATE "-fPIC")
    endfunction()
endif ()

# Protobuf/ grpc config
# See the example in the gRPC repo here:                                         
# https://github.com/grpc/grpc/blob/master/examples/cpp/helloworld/CMakeLists.txt
include(FindProtobuf)
set(protobuf_MODULE_COMPATIBLE TRUE)                                            
                                                                                 
find_package(Protobuf REQUIRED)                                                  
message(STATUS "Using protobuf  \                                                
    ${PROTOBUF_LIBRARY} \                                                        
    ${PROTOBUF_PROTOC_LIBRARY} \                                                 
    ${PROTOBUF_PROTOC_EXECUTABLE} \                                              
")                                                                               
                                                                                 
find_package(gRPC CONFIG REQUIRED)                                               
message(STATUS "Using gRPC ${gRPC_VERSION}")                                     
                                                                                 
include_directories(${PROTOBUF_INCLUDE_DIR})                                     
                                                                                 
set(PROTOC_EXE /usr/local/bin/protoc)                                            
set(GRPC_PLUGIN /usr/local/bin/grpc_cpp_plugin)                                  

# Output directories
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)

set(FAABRIC_INCLUDE_DIR ${CMAKE_CURRENT_LIST_DIR}/include)

# Include directories
include_directories(
        ${FAABRIC_INCLUDE_DIR}
        ${CMAKE_CURRENT_BINARY_DIR}/src # Required for protobuf
)

add_subdirectory(src/mpi)
add_subdirectory(src/proto)
add_subdirectory(src/redis)
add_subdirectory(src/scheduler)
add_subdirectory(src/state)
add_subdirectory(src/util)
add_subdirectory(src/wasm)


faabric_public_lib(faabric faabric.cpp)

target_link_libraries(faabric
        proto
        redis
        scheduler
        state
        util
        )

# Tests
add_library(catch INTERFACE)
add_subdirectory(tests/test)
add_subdirectory(tests/utils)
