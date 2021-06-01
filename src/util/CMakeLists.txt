find_package(RapidJSON)

file(GLOB HEADERS "${FAABRIC_INCLUDE_DIR}/faabric/util/*.h")

set(LIB_FILES
        barrier.cpp
        bytes.cpp
        config.cpp
        clock.cpp
        delta.cpp
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
        snapshot.cpp
        state.cpp
        string_tools.cpp
        timing.cpp
        testing.cpp
        ${HEADERS}
        )

faabric_lib(util "${LIB_FILES}")

add_dependencies(util pistache_ext)
add_dependencies(util spdlog_ext)
add_dependencies(util rapidjson_ext)
add_dependencies(util cppcodec_ext)

target_link_libraries(
        util PUBLIC
        proto
        boost_system
        boost_filesystem
        zstd::libzstd_static
)
