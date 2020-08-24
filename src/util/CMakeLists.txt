include_directories(${FAABRIC_INCLUDE_DIR}/faabric/util)

find_package(RapidJSON)

file(GLOB HEADERS "${FAABRIC_INCLUDE_DIR}/faabric/util/*.h")

add_library(util
        barrier.cpp
        bytes.cpp
        chaining.cpp
        config.cpp
        clock.cpp
        environment.cpp
        files.cpp
        func.cpp
        gids.cpp
        json.cpp
        logging.cpp
        memory.cpp
        network.cpp
        queue.cpp
        random.cpp
        state.cpp
        string_tools.cpp
        timing.cpp
        ${HEADERS}
        )

target_link_libraries(
        util
        proto
        ssl
        curl
        crypto
        boost_system
        boost_filesystem
)
