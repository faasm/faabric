cmake_minimum_required(VERSION 3.13.0)
project(faabric)

include(FindGit)
find_package(Git)
include (ExternalProject)

option(FAABRIC_WASM_BUILD "Build Faabric wasm library" OFF)
option(FAABRIC_STATIC_LIBS "Statically link Faabric libs" ON)
option(FAABRIC_BUILD_TESTS "Build Faabric tests" ON)

# Top-level CMake config
set(CMAKE_CXX_FLAGS_DEBUG "-g")
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

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

# Third party
set(EXTERNAL_INSTALL_DIR ${CMAKE_CURRENT_LIST_DIR}/install)
# include_directories(${EXTERNAL_INSTALL_DIR}/include)

ExternalProject_Add(cereal_ext
    GIT_REPOSITORY "https://github.com/USCiLab/cereal"
    GIT_TAG "v1.3.0"
    CMAKE_ARGS "-DJUST_INSTALL_CEREAL=1"
    CMAKE_CACHE_ARGS "-DCMAKE_INSTALL_PREFIX:STRING=${EXTERNAL_INSTALL_DIR}"
)

ExternalProject_Add(pistache_ext
    GIT_REPOSITORY "https://github.com/oktal/pistache.git"
    GIT_TAG "2ef937c434810858e05d446e97acbdd6cc1a5a36"
    CMAKE_CACHE_ARGS "-DCMAKE_INSTALL_PREFIX:STRING=${EXTERNAL_INSTALL_DIR}"
)

ExternalProject_Add(rapidjson_ext
    GIT_REPOSITORY "https://github.com/Tencent/rapidjson"
    GIT_TAG "2ce91b823c8b4504b9c40f99abf00917641cef6c"
    CMAKE_ARGS "-DRAPIDJSON_BUILD_DOC=OFF \
        -DRAPIDJSON_BUILD_EXAMPLES=OFF \
        -DRAPIDJSON_BUILD_TESTS=OFF"
    CMAKE_CACHE_ARGS "-DCMAKE_INSTALL_PREFIX:STRING=${EXTERNAL_INSTALL_DIR}"
)

ExternalProject_Add(spdlog_ext
    GIT_REPOSITORY "https://github.com/gabime/spdlog"
    GIT_TAG "v1.8.0"  
    CMAKE_CACHE_ARGS "-DCMAKE_INSTALL_PREFIX:STRING=${EXTERNAL_INSTALL_DIR}"
)

if(FAABRIC_BUILD_TESTS)
    ExternalProject_Add(catch_ext
        GIT_REPOSITORY "https://github.com/catchorg/Catch2"
        GIT_TAG "v2.13.2"
        CMAKE_ARGS "-DCATCH_INSTALL_DOCS=OFF \
            -DCATCH_INSTALL_EXTRAS=OFF"
        CMAKE_CACHE_ARGS "-DCMAKE_INSTALL_PREFIX:STRING=${EXTERNAL_INSTALL_DIR}"
    )
endif()

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

# Tests
if(FAABRIC_BUILD_TESTS)
    add_subdirectory(tests/test)
    add_subdirectory(tests/utils)
endif()

