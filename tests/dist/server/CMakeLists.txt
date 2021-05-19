include_directories(
    ${CMAKE_CURRENT_LIST_DIR}
    ${CMAKE_CURRENT_LIST_DIR}/../../utils
)

add_executable(
    faabric_dist_test_server
    server.h
    server.cpp
    ../DistTestExecutor.h
    ../DistTestExecutor.cpp
    thread_funcs.cpp
)

target_link_libraries(faabric_dist_test_server
    faabric_test_utils
)

