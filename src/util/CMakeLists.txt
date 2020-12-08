include_directories(${FAABRIC_INCLUDE_DIR}/faabric/util)

find_package(RapidJSON)

file(GLOB HEADERS "${FAABRIC_INCLUDE_DIR}/faabric/util/*.h")

set(LIB_FILES
        barrier.cpp
        bytes.cpp
        config.cpp
        clock.cpp
        environment.cpp
        files.cpp
        func.cpp
        gids.cpp
        http.cpp
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

faabric_private_lib(util "${LIB_FILES}")

add_dependencies(util spdlog_ext)
add_dependencies(util rapidjson_ext)

target_link_libraries(
        util
        proto
        boost_system
        boost_filesystem
)
