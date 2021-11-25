cmake_minimum_required(VERSION 3.21.0)
project(faabric)

option(FAABRIC_WASM_BUILD "Build Faabric wasm library" OFF)
option(FAABRIC_BUILD_TESTS "Build Faabric tests" ON)

# Enable colorized compiler output
if("${CMAKE_CXX_COMPILER_ID}" STREQUAL "GNU")
    add_compile_options(-fdiagnostics-color=always)
elseif("${CMAKE_CXX_COMPILER_ID}" STREQUAL "Clang")
    add_compile_options(-fcolor-diagnostics)
endif()

# Top-level CMake config
set(CMAKE_CXX_FLAGS "-Wall")
set(CMAKE_CXX_FLAGS_DEBUG "-g")
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXE_LINKER_FLAGS "-fuse-ld=lld")

# Compile comamnds for clang tools
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# Set-up use of sanitisers
if (FAABRIC_USE_SANITISER STREQUAL "Address")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=address")
elseif (FAABRIC_USE_SANITISER STREQUAL "Thread")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=thread")
elseif (FAABRIC_USE_SANITISER STREQUAL "Undefined")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=undefined")
elseif (FAABRIC_USE_SANITISER STREQUAL "Leak")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=leak")
elseif (FAABRIC_USE_SANITISER STREQUAL "Memory")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=memory")
endif()

# Global include dir
set(FAABRIC_INCLUDE_DIR ${CMAKE_CURRENT_LIST_DIR}/include)

# Output directories
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)

# External libraries
include(cmake/ExternalProjects.cmake)

# Library funcs
function(faabric_lib lib_name)
    # Shared dependencies between the object and normal library
    add_library(${lib_name}_deps INTERFACE)
    target_link_libraries(${lib_name}_deps INTERFACE faabric::common_dependencies)

    # Object library for bundling everything together (should have the same
    # include dirs and dependencies as the normal library)
    add_library(${lib_name}_obj OBJECT ${ARGN})
    target_link_libraries(${lib_name}_obj PUBLIC ${lib_name}_deps)
    add_library(faabric::${lib_name}_obj ALIAS ${lib_name}_obj)

    # "Normal" library used for linking internally
    add_library(${lib_name} ${ARGN})
    target_link_libraries(${lib_name} PUBLIC ${lib_name}_deps)
    add_library(faabric::${lib_name} ALIAS ${lib_name})

    if(BUILD_SHARED_LIBS)
        target_link_options(${lib_name} PRIVATE "-fuse-ld=lld")
        set_property(TARGET ${lib_name} PROPERTY POSITION_INDEPENDENT_CODE ON)
        set_property(TARGET ${lib_name}_obj PROPERTY POSITION_INDEPENDENT_CODE ON)
    endif()

    # Ensure library generates readable stack traces
    target_compile_options(${lib_name} PUBLIC -fno-omit-frame-pointer)
    target_link_options(${lib_name} PUBLIC -Wl,--export-dynamic)
endfunction()

add_subdirectory(src/endpoint)
add_subdirectory(src/flat)
add_subdirectory(src/mpi)
add_subdirectory(src/mpi_native)
add_subdirectory(src/proto)
add_subdirectory(src/redis)
add_subdirectory(src/runner)
add_subdirectory(src/scheduler)
add_subdirectory(src/snapshot)
add_subdirectory(src/state)
add_subdirectory(src/transport)
add_subdirectory(src/util)

# Wrapper library - note we want to include all the _object_ targets in this
# library to ensure it's all bundled in together
if(BUILD_SHARED_LIBS)
    set(FAABRIC_LIB_TYPE SHARED)
else()
    set(FAABRIC_LIB_TYPE STATIC)
endif()

add_library(faabric
    ${FAABRIC_LIB_TYPE}
    faabric.cpp
    $<TARGET_OBJECTS:endpoint_obj>
    $<TARGET_OBJECTS:flat_obj>
    $<TARGET_OBJECTS:proto_obj>
    $<TARGET_OBJECTS:redis_obj>
    $<TARGET_OBJECTS:runner_obj>
    $<TARGET_OBJECTS:scheduler_obj>
    $<TARGET_OBJECTS:snapshot_obj>
    $<TARGET_OBJECTS:state_obj>
    $<TARGET_OBJECTS:transport_obj>
    $<TARGET_OBJECTS:util_obj>
)
add_library(faabric::faabric ALIAS faabric)

if(BUILD_SHARED_LIBS)
    target_link_options(faabric PRIVATE "-fuse-ld=lld")
endif()

target_link_libraries(faabric PUBLIC
    faabric::faabricmpi
    faabric::common_dependencies
)

target_include_directories(faabric PUBLIC
    ${FAABRIC_INCLUDE_DIR}
    ${CMAKE_INSTALL_PREFIX}/include
)

# Ensure faabric generates readable stack traces
target_compile_options(faabric PUBLIC -fno-omit-frame-pointer)
target_link_options(faabric PUBLIC -Wl,--export-dynamic)

# Tests - only include in static builds _and_ when requested
if(BUILD_SHARED_LIBS)
    message(STATUS "Skipping test build with shared libs")
elseif(FAABRIC_BUILD_TESTS)
    add_subdirectory(tests/dist)
    add_subdirectory(tests/test)
endif()
# Utils are used by faasm tests
add_subdirectory(tests/utils)

# Install headers
install(
    DIRECTORY ${FAABRIC_INCLUDE_DIR}/faabric
    DESTINATION include
)

install(TARGETS faabric)
