set(LIB_FILES
    DummyExecutor.cpp
    DummyExecutor.h
    DummyExecutorFactory.cpp
    DummyExecutorFactory.h
    exec_graph_utils.cpp
    faabric_utils.h
    fixtures.h
    http_utils.cpp
    message_utils.cpp
    scheduling_utils.cpp
    system_utils.cpp
    )

add_library(faabric_test_utils "${LIB_FILES}")

target_compile_options(faabric_test_utils PUBLIC -fno-omit-frame-pointer)
target_link_options(faabric_test_utils PUBLIC -Wl,--export-dynamic)

add_dependencies(faabric_test_utils catch_ext)
target_include_directories(faabric_test_utils PUBLIC ${CMAKE_CURRENT_LIST_DIR})

target_link_libraries(faabric_test_utils state runner scheduler transport)
