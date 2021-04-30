set(LIB_FILES
    DummyExecutor.cpp
    DummyExecutor.h
    DummyExecutorFactory.cpp
    DummyExecutorFactory.h
    exec_graph_utils.cpp
    message_utils.cpp
    system_utils.cpp
    faabric_utils.h
    )

add_library(faabric_test_utils "${LIB_FILES}")

add_dependencies(faabric_test_utils catch_ext)
target_include_directories(faabric_test_utils PUBLIC ${CMAKE_CURRENT_LIST_DIR})

target_link_libraries(faabric_test_utils executor state endpoint scheduler)
