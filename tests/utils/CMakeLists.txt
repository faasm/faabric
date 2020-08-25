include_directories(${CMAKE_CURRENT_LIST_DIR})

add_library(faabric_test_utils SHARED
    exec_graph_utils.cpp
    message_utils.cpp
    system_utils.cpp
    faabric_utils.h
)

target_link_libraries(faabric_test_utils
    catch
    faabric
)
