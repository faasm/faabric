include(FindGit)
find_package(Git)
include (ExternalProject)
include (FetchContent)
find_package (Threads REQUIRED)

# Find conan-generated package descriptions.
list(PREPEND CMAKE_MODULE_PATH ${CMAKE_CURRENT_BINARY_DIR})
list(PREPEND CMAKE_PREFIX_PATH ${CMAKE_CURRENT_BINARY_DIR})

find_package(absl QUIET REQUIRED)
find_package(Boost 1.84.0 QUIET REQUIRED COMPONENTS filesystem program_options system)
find_package(Catch2 QUIET REQUIRED)
find_package(flatbuffers CONFIG QUIET REQUIRED)
find_package(fmt QUIET REQUIRED)
find_package(hiredis QUIET REQUIRED)
find_package(OpenSSL 3.6.0 QUIET REQUIRED)
find_package(Protobuf 6.30.1 QUIET REQUIRED)
find_package(readerwriterqueue QUIET REQUIRED)
find_package(spdlog QUIET REQUIRED)
find_package(ZLIB QUIET REQUIRED)

# --------------------------------
# Fetch content dependencies
# --------------------------------

# zstd (Conan version not customizable enough)
set(ZSTD_BUILD_CONTRIB OFF CACHE INTERNAL "")
set(ZSTD_BUILD_CONTRIB OFF CACHE INTERNAL "")
set(ZSTD_BUILD_PROGRAMS OFF CACHE INTERNAL "")
set(ZSTD_BUILD_SHARED ON CACHE INTERNAL "")
set(ZSTD_BUILD_STATIC ON CACHE INTERNAL "")
set(ZSTD_BUILD_TESTS OFF CACHE INTERNAL "")
# This means zstd doesn't use threading internally,
# not that it can't be used in a multithreaded context
set(ZSTD_MULTITHREAD_SUPPORT OFF CACHE INTERNAL "")
set(ZSTD_LEGACY_SUPPORT OFF CACHE INTERNAL "")
set(ZSTD_ZLIB_SUPPORT OFF CACHE INTERNAL "")
set(ZSTD_LZMA_SUPPORT OFF CACHE INTERNAL "")
set(ZSTD_LZ4_SUPPORT OFF CACHE INTERNAL "")

# nng (Conan version out of date)
set(NNG_TESTS OFF CACHE INTERNAL "")

FetchContent_Declare(atomic_queue_ext
    GIT_REPOSITORY "https://github.com/max0x7ba/atomic_queue"
    GIT_TAG "7c36f0997979a0fee5be84c9511ee0f6032057ec"
)
FetchContent_Declare(nng_ext
    GIT_REPOSITORY "https://github.com/nanomsg/nng"
    # NNG tagged version 1.7.1
    GIT_TAG "ec4b5722fba105e3b944e3dc0f6b63c941748b3f"
)
FetchContent_Declare(zstd_ext
    GIT_REPOSITORY "https://github.com/facebook/zstd"
    GIT_TAG "v1.5.7"
    SOURCE_SUBDIR "build/cmake"
)

FetchContent_MakeAvailable(atomic_queue_ext)
add_library(atomic_queue::atomic_queue ALIAS atomic_queue)

FetchContent_MakeAvailable(nng_ext)
add_library(nng::nng ALIAS nng)

FetchContent_MakeAvailable(zstd_ext)
# Work around zstd not declaring its targets properly
target_include_directories(libzstd_static SYSTEM INTERFACE $<BUILD_INTERFACE:${zstd_ext_SOURCE_DIR}/lib>)
target_include_directories(libzstd_shared SYSTEM INTERFACE $<BUILD_INTERFACE:${zstd_ext_SOURCE_DIR}/lib>)
add_library(zstd::libzstd_static ALIAS libzstd_static)
add_library(zstd::libzstd_shared ALIAS libzstd_shared)

# Group all external dependencies into a convenient virtual CMake library
add_library(faabric_common_dependencies INTERFACE)
target_include_directories(faabric_common_dependencies INTERFACE
    ${FAABRIC_INCLUDE_DIR}
)
target_link_libraries(faabric_common_dependencies INTERFACE
    absl::algorithm_container
    absl::btree
    absl::flat_hash_set
    absl::flat_hash_map
    absl::strings
    atomic_queue::atomic_queue
    Boost::headers
    Boost::filesystem
    Boost::program_options
    Boost::system
    flatbuffers::flatbuffers
    hiredis::hiredis
    nng::nng
    protobuf::libprotobuf
    readerwriterqueue::readerwriterqueue
    spdlog::spdlog
    Threads::Threads
    zstd::libzstd_static
)
target_compile_definitions(faabric_common_dependencies INTERFACE
    FMT_DEPRECATED= # Suppress warnings about use of deprecated api by spdlog
)
add_library(faabric::common_dependencies ALIAS faabric_common_dependencies)
