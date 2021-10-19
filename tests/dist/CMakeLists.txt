file(GLOB_RECURSE TEST_FILES ${CMAKE_CURRENT_LIST_DIR} test_*.cpp)

include_directories(
    ${CMAKE_CURRENT_LIST_DIR}
    ${CMAKE_CURRENT_LIST_DIR}/../utils
)

# Shared
add_library(faabric_dist_tests_lib
    init.h
    init.cpp
    DistTestExecutor.h
    DistTestExecutor.cpp
    scheduler/functions.cpp
    transport/functions.cpp
    )

target_link_libraries(faabric_dist_tests_lib faabric_test_utils)

# Tests
add_executable(
    faabric_dist_tests
    main.cpp
    ${TEST_FILES}
)

target_link_libraries(faabric_dist_tests faabric_dist_tests_lib)

add_test(NAME faabric_dist_tests COMMAND "tests/dist/faabric_dist_tests")

# Server
add_executable(
    faabric_dist_test_server
    server.cpp
)

target_link_libraries(faabric_dist_test_server faabric_dist_tests_lib)

