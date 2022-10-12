include(FindGit)
find_package(Git)
include (ExternalProject)
include (FetchContent)
find_package (Threads REQUIRED)

# Find conan-generated package descriptions
list(PREPEND CMAKE_MODULE_PATH ${CMAKE_CURRENT_BINARY_DIR})
list(PREPEND CMAKE_PREFIX_PATH ${CMAKE_CURRENT_BINARY_DIR})

if(NOT EXISTS "${CMAKE_CURRENT_BINARY_DIR}/conan.cmake")
  message(STATUS "Downloading conan.cmake from https://github.com/conan-io/cmake-conan")
  file(DOWNLOAD "https://raw.githubusercontent.com/conan-io/cmake-conan/0.18.1/conan.cmake"
                "${CMAKE_CURRENT_BINARY_DIR}/conan.cmake"
                TLS_VERIFY ON)
endif()

include(${CMAKE_CURRENT_BINARY_DIR}/conan.cmake)

conan_check(VERSION 1.52.0 REQUIRED)

# Enable revisions in the conan config
execute_process(COMMAND ${CONAN_CMD} config set general.revisions_enabled=1
                RESULT_VARIABLE RET_CODE)
if(NOT "${RET_CODE}" STREQUAL "0")
    message(FATAL_ERROR "Error setting revisions for Conan: '${RET_CODE}'")
endif()

conan_cmake_configure(
    REQUIRES
        "boost/1.80.0@#db5db5bd811d23b95089d4a95259d147"
        "catch2/2.13.9@#8793d3e6287d3684201418de556d98fe"
        "cppcodec/0.2@#f6385611ce2f7cff954ac8b16e25c4fa"
        "cpprestsdk/2.10.18@#ed9788e9d202d6eadd92581368ddfc2f"
        "cppzmq/4.8.1@#010df8fa1c5ebbc615704e8c16693bac"
        "flatbuffers/2.0.5@#c6a9508bd476da080f7aecbe7a094b68"
        "hiredis/1.0.2@#370dad964286cadb1f15dc90252e8ef3"
        "protobuf/3.20.0@#8e4de7081bea093469c9e6076149b2b4"
        "rapidjson/cci.20211112@#65b4e5feb6f1edfc8cbac0f669acaf17"
        "readerwriterqueue/1.0.6@#a95c8da3d68822dec4d4c13fff4b5c96"
        "spdlog/1.10.0@#6406c337028e15e56cd6a070cbac54c4"
        "zeromq/4.3.4@#d4fe4001f6c2e5960e58c251687c5b2f"
        "zlib/1.2.12@#3b9e037ae1c615d045a06c67d88491ae"
    GENERATORS
        cmake_find_package
        cmake_paths
    OPTIONS
        flatbuffers:options_from_context=False
        flatbuffers:flatc=True
        flatbuffers:flatbuffers=True
        boost:error_code_header_only=True
        boost:system_no_deprecated=True
        boost:filesystem_no_deprecated=True
        boost:zlib=False
        boost:bzip2=False
        boost:lzma=False
        boost:zstd=False
        boost:without_locale=True
        boost:without_log=True
        boost:without_mpi=True
        boost:without_python=True
        boost:without_test=True
        boost:without_wave=True
        cpprestsdk:with_websockets=False
        zeromq:encryption=None
)

conan_cmake_autodetect(FAABRIC_CONAN_SETTINGS)

conan_cmake_install(PATH_OR_REFERENCE .
                    BUILD outdated
                    UPDATE
                    REMOTE conancenter
                    PROFILE_HOST ${CMAKE_CURRENT_LIST_DIR}/../conan-profile.txt
                    PROFILE_BUILD ${CMAKE_CURRENT_LIST_DIR}/../conan-profile.txt
                    SETTINGS ${FAABRIC_CONAN_SETTINGS}
)

include(${CMAKE_CURRENT_BINARY_DIR}/conan_paths.cmake)

find_package(Boost 1.79.0 REQUIRED)
find_package(Catch2 REQUIRED)
find_package(FlatBuffers REQUIRED)
find_package(Protobuf 3.20.0 REQUIRED)
find_package(RapidJSON REQUIRED)
find_package(ZLIB REQUIRED)
find_package(ZeroMQ REQUIRED)
find_package(cppcodec REQUIRED)
find_package(cpprestsdk REQUIRED)
find_package(cppzmq REQUIRED)
find_package(fmt REQUIRED)
find_package(hiredis REQUIRED)
find_package(spdlog REQUIRED)
find_package(readerwriterqueue REQUIRED)

# zstd (Conan version not customizable enough)
set(ZSTD_BUILD_CONTRIB OFF CACHE INTERNAL "")
set(ZSTD_BUILD_CONTRIB OFF CACHE INTERNAL "")
set(ZSTD_BUILD_PROGRAMS OFF CACHE INTERNAL "")
set(ZSTD_BUILD_SHARED OFF CACHE INTERNAL "")
set(ZSTD_BUILD_STATIC ON CACHE INTERNAL "")
set(ZSTD_BUILD_TESTS OFF CACHE INTERNAL "")
# This means zstd doesn't use threading internally,
# not that it can't be used in a multithreaded context
set(ZSTD_MULTITHREAD_SUPPORT OFF CACHE INTERNAL "")
set(ZSTD_LEGACY_SUPPORT OFF CACHE INTERNAL "")
set(ZSTD_ZLIB_SUPPORT OFF CACHE INTERNAL "")
set(ZSTD_LZMA_SUPPORT OFF CACHE INTERNAL "")
set(ZSTD_LZ4_SUPPORT OFF CACHE INTERNAL "")

FetchContent_Declare(zstd_ext
    GIT_REPOSITORY "https://github.com/facebook/zstd"
    GIT_TAG "v1.5.2"
    SOURCE_SUBDIR "build/cmake"
)

FetchContent_MakeAvailable(zstd_ext)
# Work around zstd not declaring its targets properly
target_include_directories(libzstd_static SYSTEM INTERFACE $<BUILD_INTERFACE:${zstd_ext_SOURCE_DIR}/lib>)
add_library(zstd::libzstd_static ALIAS libzstd_static)

# Group all external dependencies into a convenient virtual CMake library
add_library(faabric_common_dependencies INTERFACE)
target_include_directories(faabric_common_dependencies INTERFACE
    ${FAABRIC_INCLUDE_DIR}
)
target_link_libraries(faabric_common_dependencies INTERFACE
    Boost::Boost
    Boost::filesystem
    Boost::system
    cppcodec::cppcodec
    cpprestsdk::cpprestsdk
    cppzmq::cppzmq
    flatbuffers::flatbuffers
    hiredis::hiredis
    protobuf::libprotobuf
    RapidJSON::RapidJSON
    readerwriterqueue::readerwriterqueue
    spdlog::spdlog
    Threads::Threads
    zstd::libzstd_static
)
target_compile_definitions(faabric_common_dependencies INTERFACE
    FMT_DEPRECATED= # Suppress warnings about use of deprecated api by spdlog
    BOOST_NO_TYPEID=1 # Prevent odd crashes within asio implementation
)
add_library(faabric::common_dependencies ALIAS faabric_common_dependencies)
