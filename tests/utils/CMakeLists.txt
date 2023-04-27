
add_library(faabric_test_utils
    DummyExecutor.cpp
    DummyExecutorFactory.cpp
    exec_graph_utils.cpp
    http_utils.cpp
    message_utils.cpp
    planner_utils.cpp
    scheduling_utils.cpp
    system_utils.cpp
)

target_compile_options(faabric_test_utils PUBLIC -fno-omit-frame-pointer)
target_link_options(faabric_test_utils PUBLIC -Wl,--export-dynamic)

target_include_directories(faabric_test_utils PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

target_link_libraries(faabric_test_utils PUBLIC
    faabric::common_dependencies
    faabric::faabric
    curl
    Catch2::Catch2
)

add_library(faabric::test_utils ALIAS faabric_test_utils)
